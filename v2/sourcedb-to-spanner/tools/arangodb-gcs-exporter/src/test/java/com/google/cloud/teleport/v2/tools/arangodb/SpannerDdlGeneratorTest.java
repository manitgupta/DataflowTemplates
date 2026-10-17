/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.tools.arangodb;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerDdlGeneratorTest {

  private static final List<CollectionPlan> PLANS =
      Arrays.asList(
          CollectionPlan.create(
              new CollectionInfo("user-profiles", CollectionKind.DOCUMENT), "user_profiles", 1),
          CollectionPlan.create(new CollectionInfo("knows", CollectionKind.EDGE), "knows", 1));

  @Test
  public void googleSqlWithEdgeHelpers() {
    String expected =
        "CREATE TABLE `user_profiles` (\n"
            + "  `key` STRING(254) NOT NULL,\n"
            + "  `id` STRING(MAX),\n"
            + "  `rev` STRING(64),\n"
            + "  `doc` JSON NOT NULL\n"
            + ") PRIMARY KEY (`key`);\n"
            + "\n"
            + "CREATE TABLE `knows` (\n"
            + "  `key` STRING(254) NOT NULL,\n"
            + "  `id` STRING(MAX),\n"
            + "  `rev` STRING(64),\n"
            + "  `from_id` STRING(MAX) NOT NULL,\n"
            + "  `to_id` STRING(MAX) NOT NULL,\n"
            + "  `doc` JSON NOT NULL,\n"
            + "  `from_coll` STRING(MAX) AS (REGEXP_EXTRACT(`from_id`, r'^([^/]+)/')) STORED,\n"
            + "  `from_key` STRING(254) AS (REGEXP_EXTRACT(`from_id`, r'^[^/]+/(.*)$')) STORED,\n"
            + "  `to_coll` STRING(MAX) AS (REGEXP_EXTRACT(`to_id`, r'^([^/]+)/')) STORED,\n"
            + "  `to_key` STRING(254) AS (REGEXP_EXTRACT(`to_id`, r'^[^/]+/(.*)$')) STORED\n"
            + ") PRIMARY KEY (`key`);\n"
            + "CREATE INDEX `knows_by_from` ON `knows`(`from_id`);\n"
            + "CREATE INDEX `knows_by_to` ON `knows`(`to_id`);\n";
    assertThat(SpannerDdlGenerator.googleSql(PLANS, true)).isEqualTo(expected);
  }

  @Test
  public void googleSqlWithoutEdgeHelpers() {
    String expected =
        "CREATE TABLE `user_profiles` (\n"
            + "  `key` STRING(254) NOT NULL,\n"
            + "  `id` STRING(MAX),\n"
            + "  `rev` STRING(64),\n"
            + "  `doc` JSON NOT NULL\n"
            + ") PRIMARY KEY (`key`);\n"
            + "\n"
            + "CREATE TABLE `knows` (\n"
            + "  `key` STRING(254) NOT NULL,\n"
            + "  `id` STRING(MAX),\n"
            + "  `rev` STRING(64),\n"
            + "  `from_id` STRING(MAX) NOT NULL,\n"
            + "  `to_id` STRING(MAX) NOT NULL,\n"
            + "  `doc` JSON NOT NULL\n"
            + ") PRIMARY KEY (`key`);\n";
    assertThat(SpannerDdlGenerator.googleSql(PLANS, false)).isEqualTo(expected);
  }

  @Test
  public void postgreSql() {
    String expected =
        "CREATE TABLE \"user_profiles\" (\"key\" varchar(254) NOT NULL, \"id\" text, \"rev\""
            + " varchar(64), \"doc\" jsonb NOT NULL, PRIMARY KEY (\"key\"));\n"
            + "CREATE TABLE \"knows\" (\"key\" varchar(254) NOT NULL, \"id\" text, \"rev\""
            + " varchar(64), \"from_id\" text NOT NULL, \"to_id\" text NOT NULL, \"doc\" jsonb NOT"
            + " NULL, PRIMARY KEY (\"key\"));\n"
            + "CREATE INDEX \"knows_by_from\" ON \"knows\"(\"from_id\");\n"
            + "CREATE INDEX \"knows_by_to\" ON \"knows\"(\"to_id\");\n";
    assertThat(SpannerDdlGenerator.postgreSql(PLANS)).isEqualTo(expected);
  }

  @Test
  public void longIndexNamesAreTruncated() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      sb.append('e');
    }
    String name = SpannerDdlGenerator.indexName(sb.toString(), "from");
    assertThat(name.length()).isAtMost(128);
    assertThat(name).endsWith("_by_from");
  }

  private static CollectionPlan edge(String table) {
    return CollectionPlan.create(new CollectionInfo(table, CollectionKind.EDGE), table, 1);
  }

  private static CollectionPlan doc(String table) {
    return CollectionPlan.create(new CollectionInfo(table, CollectionKind.DOCUMENT), table, 1);
  }

  @Test
  public void indexNamesCollidingWithTablesOrEachOtherGetHashSuffix() {
    StringBuilder common = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      common.append('e');
    }
    List<CollectionPlan> plans =
        Arrays.asList(
            edge("knows"),
            doc("knows_by_from"),
            edge(common + "_one"),
            edge(common + "_two"),
            edge("likes"));
    Map<String, String[]> names = SpannerDdlGenerator.indexNames(plans);

    Set<String> all = new HashSet<>();
    for (CollectionPlan p : plans) {
      all.add(p.tableName().toLowerCase());
    }
    for (String[] pair : names.values()) {
      for (String n : pair) {
        assertThat(n.length()).isAtMost(128);
        assertWithMessage(n).that(all.add(n.toLowerCase())).isTrue();
      }
    }
    assertThat(names.get("knows")[0]).matches("knows_[0-9a-f]{8}_by_from");
    assertThat(names.get("likes")).asList().containsExactly("likes_by_from", "likes_by_to");
    assertThat(names.get(common + "_one")[0]).isEqualTo(common + "_by_from");
    assertThat(names.get(common + "_two")[0]).matches("e{111}_[0-9a-f]{8}_by_from");

    String ddl = SpannerDdlGenerator.googleSql(plans, true);
    assertThat(ddl).contains("CREATE INDEX `" + names.get("knows")[0] + "` ON `knows`");
    assertThat(SpannerDdlGenerator.postgreSql(plans))
        .contains("CREATE INDEX \"" + names.get("knows")[1] + "\" ON \"knows\"");
  }
}
