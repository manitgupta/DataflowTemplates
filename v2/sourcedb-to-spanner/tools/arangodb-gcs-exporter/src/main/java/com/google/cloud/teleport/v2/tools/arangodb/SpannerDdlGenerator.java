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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Generates suggested Spanner DDL (GoogleSQL and PostgreSQL dialects) for the exported envelope
 * tables. Identifiers are always quoted. Statements end with {@code ;\n} so the files can be passed
 * to {@code gcloud spanner databases create --ddl-file} and Spanner Omni's equivalent. The files
 * contain no comments, which keeps them safe for naive statement splitters.
 */
public final class SpannerDdlGenerator {

  public static final String GOOGLESQL_FILE = "spanner-ddl-googlesql.sql";
  public static final String POSTGRESQL_FILE = "spanner-ddl-postgresql.sql";

  private static final int MAX_INDEX_PREFIX = 120;

  private SpannerDdlGenerator() {}

  /** GoogleSQL DDL; edge tables get generated {@code from/to} helper columns and indexes. */
  public static String googleSql(List<CollectionPlan> plans, boolean edgeHelpers) {
    Map<String, String[]> indexes = indexNames(plans);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < plans.size(); i++) {
      CollectionPlan p = plans.get(i);
      String t = gq(p.tableName());
      if (i > 0) {
        sb.append('\n');
      }
      sb.append("CREATE TABLE ").append(t).append(" (\n");
      sb.append("  `key` STRING(254) NOT NULL,\n");
      sb.append("  `id` STRING(MAX),\n");
      sb.append("  `rev` STRING(64),\n");
      if (p.kind() == CollectionKind.EDGE) {
        sb.append("  `from_id` STRING(MAX) NOT NULL,\n");
        sb.append("  `to_id` STRING(MAX) NOT NULL,\n");
      }
      if (p.kind() == CollectionKind.EDGE && edgeHelpers) {
        sb.append("  `doc` JSON NOT NULL,\n");
        sb.append(
            "  `from_coll` STRING(MAX) AS (REGEXP_EXTRACT(`from_id`, r'^([^/]+)/')) STORED,\n");
        sb.append(
            "  `from_key` STRING(254) AS (REGEXP_EXTRACT(`from_id`, r'^[^/]+/(.*)$')) STORED,\n");
        sb.append("  `to_coll` STRING(MAX) AS (REGEXP_EXTRACT(`to_id`, r'^([^/]+)/')) STORED,\n");
        sb.append("  `to_key` STRING(254) AS (REGEXP_EXTRACT(`to_id`, r'^[^/]+/(.*)$')) STORED\n");
      } else {
        sb.append("  `doc` JSON NOT NULL\n");
      }
      sb.append(") PRIMARY KEY (`key`);\n");
      if (p.kind() == CollectionKind.EDGE && edgeHelpers) {
        sb.append("CREATE INDEX ")
            .append(gq(indexes.get(p.tableName())[0]))
            .append(" ON ")
            .append(t)
            .append("(`from_id`);\n");
        sb.append("CREATE INDEX ")
            .append(gq(indexes.get(p.tableName())[1]))
            .append(" ON ")
            .append(t)
            .append("(`to_id`);\n");
      }
    }
    return sb.toString();
  }

  /** PostgreSQL-dialect DDL; edge tables get indexes on {@code from_id} and {@code to_id}. */
  public static String postgreSql(List<CollectionPlan> plans) {
    Map<String, String[]> indexes = indexNames(plans);
    StringBuilder sb = new StringBuilder();
    for (CollectionPlan p : plans) {
      String t = pq(p.tableName());
      sb.append("CREATE TABLE ")
          .append(t)
          .append(" (\"key\" varchar(254) NOT NULL, \"id\" text, \"rev\" varchar(64), ");
      if (p.kind() == CollectionKind.EDGE) {
        sb.append("\"from_id\" text NOT NULL, \"to_id\" text NOT NULL, ");
      }
      sb.append("\"doc\" jsonb NOT NULL, PRIMARY KEY (\"key\"));\n");
      if (p.kind() == CollectionKind.EDGE) {
        sb.append("CREATE INDEX ")
            .append(pq(indexes.get(p.tableName())[0]))
            .append(" ON ")
            .append(t)
            .append("(\"from_id\");\n");
        sb.append("CREATE INDEX ")
            .append(pq(indexes.get(p.tableName())[1]))
            .append(" ON ")
            .append(t)
            .append("(\"to_id\");\n");
      }
    }
    return sb.toString();
  }

  static String indexName(String table, String side) {
    return indexName(table, side, "");
  }

  private static String indexName(String table, String side, String suffix) {
    int max = MAX_INDEX_PREFIX - suffix.length();
    String prefix = table.length() > max ? table.substring(0, max) : table;
    return prefix + suffix + "_by_" + side;
  }

  /**
   * The {@code from}/{@code to} index names of every edge table, unique among themselves and
   * distinct from every table name (case-insensitive, as Spanner compares names). A colliding pair
   * gets a stable {@code _<sha1(table)[0..8]>} suffix.
   *
   * @return table name to {@code [fromIndex, toIndex]}
   * @throws IllegalArgumentException if names still collide after adding the suffix
   */
  static Map<String, String[]> indexNames(List<CollectionPlan> plans) {
    Set<String> used = new HashSet<>();
    for (CollectionPlan p : plans) {
      used.add(p.tableName().toLowerCase(Locale.ROOT));
    }
    Map<String, String[]> result = new HashMap<>();
    for (CollectionPlan p : plans) {
      if (p.kind() != CollectionKind.EDGE) {
        continue;
      }
      String table = p.tableName();
      String[] names = {indexName(table, "from"), indexName(table, "to")};
      if (collides(names, used)) {
        String suffix = "_" + sha1Hex(table).substring(0, 8);
        names = new String[] {indexName(table, "from", suffix), indexName(table, "to", suffix)};
        if (collides(names, used)) {
          throw new IllegalArgumentException(
              "Cannot generate unique index names for edge table '"
                  + table
                  + "'; rename or export the colliding collections separately");
        }
      }
      used.add(names[0].toLowerCase(Locale.ROOT));
      used.add(names[1].toLowerCase(Locale.ROOT));
      result.put(table, names);
    }
    return result;
  }

  private static boolean collides(String[] names, Set<String> used) {
    return used.contains(names[0].toLowerCase(Locale.ROOT))
        || used.contains(names[1].toLowerCase(Locale.ROOT));
  }

  private static String sha1Hex(String s) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : d) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String gq(String identifier) {
    return "`" + identifier + "`";
  }

  private static String pq(String identifier) {
    return "\"" + identifier + "\"";
  }
}
