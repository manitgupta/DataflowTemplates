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
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExportSummaryWriterTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  /** Builds an example export summary (matches {@code golden/arangodb-export.json}). */
  static ExportSummary example() {
    ExportSummary m = new ExportSummary();
    m.exportId = "20261017T100000Z";
    m.producer = "arangodb-gcs-exporter/1.0.0 (spark 3.5.3, connector 1.10.0)";
    m.source = new ExportSummary.Source();
    m.source.system = "arangodb";
    m.source.database = "shop";
    m.source.serverVersion = "3.12.4";
    m.source.exportStartedAt = "2026-10-17T10:00:00Z";
    m.source.exportFinishedAt = "2026-10-17T10:04:12Z";
    m.source.consistent = true;

    ExportSummary.Table users = new ExportSummary.Table();
    users.name = "user_profiles";
    users.sourceName = "user-profiles";
    users.collectionType = "DOCUMENT";
    users.primaryKey = new ArrayList<>(Arrays.asList("key"));
    users.columns =
        new ArrayList<>(
            CollectionPlan.create(
                    new CollectionInfo("user-profiles", CollectionKind.DOCUMENT),
                    "user_profiles",
                    1)
                .columns());
    users.rowCount = 1000L;
    users.sourceCountBefore = 1000L;
    users.sourceCountAfter = 1000L;
    users.sourceRevisionBefore = "_iK3x9--_-";
    users.sourceRevisionAfter = "_iK3x9--_-";
    users.files =
        new ArrayList<>(
            Arrays.asList(
                new ExportSummary.FileEntry(
                    "user_profiles/b36539a80425c7f2c6178856a7bb935f-00000-of-00002.avro", 512),
                new ExportSummary.FileEntry(
                    "user_profiles/b36539a80425c7f2c6178856a7bb935f-00001-of-00002.avro", 488)));
    users.stats = new ExportSummary.Stats(20311, new ArrayList<>());

    ExportSummary.Table knows = new ExportSummary.Table();
    knows.name = "knows";
    knows.sourceName = "knows";
    knows.collectionType = "EDGE";
    knows.primaryKey = new ArrayList<>(Arrays.asList("key"));
    knows.columns =
        new ArrayList<>(
            CollectionPlan.create(new CollectionInfo("knows", CollectionKind.EDGE), "knows", 1)
                .columns());
    knows.rowCount = 3L;
    knows.sourceCountBefore = 3L;
    knows.sourceCountAfter = 3L;
    knows.files =
        new ArrayList<>(
            Arrays.asList(
                new ExportSummary.FileEntry(
                    "knows/a7f80905c6bc6e9912edef8c18d80a52-00000-of-00001.avro", 3)));
    knows.stats = new ExportSummary.Stats(140, new ArrayList<>());

    m.tables.add(users);
    m.tables.add(knows);
    return m;
  }

  private static String golden() throws Exception {
    try (InputStream in =
        ExportSummaryWriterTest.class.getResourceAsStream("/golden/arangodb-export.json")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void toJsonMatchesGolden() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = ExportSummaryWriter.toJson(example());
    assertThat(mapper.readTree(json)).isEqualTo(mapper.readTree(golden()));
    // Field order follows the contract.
    assertThat(json.indexOf("\"formatVersion\"")).isLessThan(json.indexOf("\"tables\""));
    assertThat(json).doesNotContain("filePattern");
    assertThat(json).doesNotContain(": null");
  }

  @Test
  public void roundTrip() throws Exception {
    ExportSummary parsed = ExportSummaryWriter.fromJson(golden());
    assertThat(parsed.formatVersion).isEqualTo(2);
    assertThat(parsed.fileFormat).isEqualTo("AVRO");
    assertThat(parsed.tables).hasSize(2);
    assertThat(parsed.tables.get(1).files.get(0).rows).isEqualTo(3);
    assertThat(parsed.tables.get(1).sourceRevisionBefore).isNull();
    ObjectMapper mapper = new ObjectMapper();
    assertThat(mapper.readTree(ExportSummaryWriter.toJson(parsed)))
        .isEqualTo(mapper.readTree(golden()));
  }

  @Test
  public void writeOnceToFileUri() throws Exception {
    Configuration conf = new Configuration();
    String root = tmp.newFolder("exp").toURI().toString().replaceAll("/$", "");
    assertThat(ExportSummaryWriter.exists(root, conf)).isFalse();
    ExportSummaryWriter.write(example(), root, conf);
    assertThat(ExportSummaryWriter.exists(root, conf)).isTrue();
    String written = HadoopFiles.readString(root + "/" + ExportSummaryWriter.SUMMARY_FILE, conf);
    ObjectMapper mapper = new ObjectMapper();
    assertThat(mapper.readTree(written)).isEqualTo(mapper.readTree(golden()));
    assertThrows(
        UncheckedIOException.class, () -> ExportSummaryWriter.write(example(), root, conf));
  }
}
