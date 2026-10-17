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
public class ManifestWriterTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  /** Builds exactly the example manifest of the design (section 3.2). */
  static ExportManifest example() {
    ExportManifest m = new ExportManifest();
    m.exportId = "20261017T100000Z";
    m.producer = "arangodb-gcs-exporter/1.0.0 (spark 3.5.3, connector 1.10.0)";
    m.source = new ExportManifest.Source();
    m.source.system = "arangodb";
    m.source.database = "shop";
    m.source.serverVersion = "3.12.4";
    m.source.exportStartedAt = "2026-10-17T10:00:00Z";
    m.source.exportFinishedAt = "2026-10-17T10:04:12Z";
    m.source.consistent = true;

    ExportManifest.Table users = new ExportManifest.Table();
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
                new ExportManifest.FileEntry(
                    "tables/user_profiles/part-00000-1b2c-c000.snappy.avro", 512),
                new ExportManifest.FileEntry(
                    "tables/user_profiles/part-00001-1b2c-c000.snappy.avro", 488)));
    users.stats = new ExportManifest.Stats(20311, new ArrayList<>());

    ExportManifest.Table knows = new ExportManifest.Table();
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
                new ExportManifest.FileEntry("tables/knows/part-00000-77aa-c000.snappy.avro", 3)));
    knows.stats = new ExportManifest.Stats(140, new ArrayList<>());

    m.tables.add(users);
    m.tables.add(knows);
    return m;
  }

  private static String golden() throws Exception {
    try (InputStream in = ManifestWriterTest.class.getResourceAsStream("/golden/manifest.json")) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void toJsonMatchesGolden() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = ManifestWriter.toJson(example());
    assertThat(mapper.readTree(json)).isEqualTo(mapper.readTree(golden()));
    // Field order follows the contract.
    assertThat(json.indexOf("\"formatVersion\"")).isLessThan(json.indexOf("\"tables\""));
    assertThat(json).doesNotContain("filePattern");
    assertThat(json).doesNotContain(": null");
  }

  @Test
  public void roundTrip() throws Exception {
    ExportManifest parsed = ManifestWriter.fromJson(golden());
    assertThat(parsed.formatVersion).isEqualTo(1);
    assertThat(parsed.fileFormat).isEqualTo("AVRO");
    assertThat(parsed.tables).hasSize(2);
    assertThat(parsed.tables.get(1).files.get(0).rows).isEqualTo(3);
    assertThat(parsed.tables.get(1).sourceRevisionBefore).isNull();
    ObjectMapper mapper = new ObjectMapper();
    assertThat(mapper.readTree(ManifestWriter.toJson(parsed))).isEqualTo(mapper.readTree(golden()));
  }

  @Test
  public void writeOnceToFileUri() throws Exception {
    Configuration conf = new Configuration();
    String root = tmp.newFolder("exp").toURI().toString().replaceAll("/$", "");
    assertThat(ManifestWriter.exists(root, conf)).isFalse();
    ManifestWriter.write(example(), root, conf);
    assertThat(ManifestWriter.exists(root, conf)).isTrue();
    String written = HadoopFiles.readString(root + "/manifest.json", conf);
    ObjectMapper mapper = new ObjectMapper();
    assertThat(mapper.readTree(written)).isEqualTo(mapper.readTree(golden()));
    assertThrows(UncheckedIOException.class, () -> ManifestWriter.write(example(), root, conf));
  }
}
