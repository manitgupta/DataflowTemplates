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
package com.google.cloud.teleport.v2.source.gcs.manifest;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import java.io.File;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsExportManifest}. */
@RunWith(JUnit4.class)
public class GcsExportManifestTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setUpFileSystems() {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
  }

  private static String valid() throws Exception {
    return GcsTestFixtures.resource("GcsSourceUT/manifest-valid.json");
  }

  private static String mutate(Consumer<ObjectNode> mutation) throws Exception {
    ObjectNode root = (ObjectNode) MAPPER.readTree(valid());
    mutation.accept(root);
    return MAPPER.writeValueAsString(root);
  }

  private static ObjectNode table(ObjectNode root, int i) {
    return (ObjectNode) root.get("tables").get(i);
  }

  private static void assertInvalid(Consumer<ObjectNode> mutation, String messagePart)
      throws Exception {
    String json = mutate(mutation);
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> GcsExportManifest.parse(json));
    assertThat(e).hasMessageThat().contains(messagePart);
  }

  @Test
  public void testParseValid() throws Exception {
    GcsExportManifest m = GcsExportManifest.parse(valid());
    assertThat(m.getFormatVersion()).isEqualTo(1);
    assertThat(m.getFileFormat()).isEqualTo("AVRO");
    assertThat(m.getExportId()).isEqualTo("20261017T100000Z");
    assertThat(m.getProducer()).startsWith("arangodb-gcs-exporter");
    assertThat(m.getSource().getSystem()).isEqualTo("arangodb");
    assertThat(m.getSource().getDatabase()).isEqualTo("shop");
    assertThat(m.getSource().getServerVersion()).isEqualTo("3.12.4");
    assertThat(m.getSource().getExportFinishedAt()).isEqualTo("2026-10-17T10:04:12Z");
    assertThat(m.getSource().getConsistent()).isTrue();
    assertThat(m.getTables()).hasSize(2);

    GcsTableManifest users = m.getTables().get(0);
    assertThat(users.getName()).isEqualTo("user_profiles");
    assertThat(users.getSourceName()).isEqualTo("user-profiles");
    assertThat(users.getCollectionType()).isEqualTo("DOCUMENT");
    assertThat(users.getPrimaryKey()).containsExactly("key");
    assertThat(users.getRowCount()).isEqualTo(1000L);
    assertThat(users.getFilePattern()).isNull();
    assertThat(users.getFiles()).hasSize(2);
    assertThat(users.getFiles().get(1).getPath())
        .isEqualTo("tables/user_profiles/part-00001-1b2c-c000.snappy.avro");
    assertThat(users.getFiles().get(1).getRows()).isEqualTo(488L);
    GcsColumnManifest doc = users.getColumns().get(3);
    assertThat(doc.getName()).isEqualTo("doc");
    assertThat(doc.getType()).isEqualTo("JSON");
    assertThat(doc.getNullable()).isFalse();
    assertThat(doc.getSourceName()).isEqualTo("$");

    GcsTableManifest knows = m.getTables().get(1);
    assertThat(knows.getColumns()).hasSize(6);
    assertThat(knows.getCollectionType()).isEqualTo("EDGE");
  }

  @Test
  public void testResolveFilePatterns() throws Exception {
    GcsExportManifest m = GcsExportManifest.parse(valid());
    assertThat(m.getTables().get(0).resolveFilePatterns("gs://my-bucket/exports/x/manifest.json"))
        .containsExactly(
            "gs://my-bucket/exports/x/tables/user_profiles/part-00000-1b2c-c000.snappy.avro",
            "gs://my-bucket/exports/x/tables/user_profiles/part-00001-1b2c-c000.snappy.avro")
        .inOrder();

    GcsExportManifest fp =
        GcsExportManifest.parse(GcsTestFixtures.resource("GcsSourceUT/manifest-filepattern.json"));
    assertThat(fp.exportIdOrDefault()).isEqualTo("fp-export");
    assertThat(fp.getTables().get(1).getFiles()).isNull();
    assertThat(fp.getTables().get(1).resolveFilePatterns("/data/exp/manifest.json"))
        .containsExactly("/data/exp/tables/knows/*.avro");
  }

  @Test
  public void testResolveFilesRejectsGlobMetacharacters() throws Exception {
    GcsTableManifest users = GcsExportManifest.parse(valid()).getTables().get(0);
    for (String dir : new String[] {"run[1]", "run*", "run?", "run{a,b}"}) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> users.resolveFilePatterns("gs://my-bucket/exports/" + dir + "/manifest.json"));
      assertThat(e).hasMessageThat().contains("glob metacharacter");
    }
  }

  @Test
  public void testReadFromFileSystem() throws Exception {
    File f = tmp.newFile("manifest.json");
    Files.write(f.toPath(), valid().getBytes(StandardCharsets.UTF_8));
    GcsExportManifest m = GcsExportManifest.read(f.getAbsolutePath());
    assertThat(m.getTables()).hasSize(2);
  }

  @Test
  public void testReadMissingFileThrows() {
    String missing = new File(tmp.getRoot(), "nope/manifest.json").getAbsolutePath();
    assertThrows(UncheckedIOException.class, () -> GcsExportManifest.read(missing));
  }

  @Test
  public void testBadVersionResource() throws Exception {
    String json = GcsTestFixtures.resource("GcsSourceUT/manifest-bad-version.json");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> GcsExportManifest.parse(json));
    assertThat(e).hasMessageThat().contains("formatVersion 2");
  }

  @Test
  public void testMalformedJsonThrows() {
    assertThrows(IllegalArgumentException.class, () -> GcsExportManifest.parse("{not json"));
    assertThrows(IllegalArgumentException.class, () -> GcsExportManifest.parse("null"));
  }

  @Test
  public void testValidationRules() throws Exception {
    assertInvalid(r -> r.put("formatVersion", 2), "formatVersion");
    assertInvalid(r -> r.put("fileFormat", "PARQUET"), "fileFormat");
    assertInvalid(r -> r.remove("fileFormat"), "fileFormat");
    assertInvalid(r -> r.putArray("tables"), "at least one table");
    assertInvalid(r -> r.remove("tables"), "at least one table");
    assertInvalid(r -> table(r, 1).put("name", "USER_PROFILES"), "Duplicate manifest table");
    assertInvalid(r -> table(r, 0).put("name", "a-b"), "a-b");
    assertInvalid(r -> table(r, 0).put("name", " "), "blank");
    assertInvalid(r -> table(r, 0).putArray("columns"), "at least one column");
    assertInvalid(
        r -> ((ObjectNode) table(r, 0).get("columns").get(1)).put("name", "key"),
        "duplicate column key");
    assertInvalid(
        r -> ((ObjectNode) table(r, 0).get("columns").get(1)).put("name", "1bad"), "1bad");
    assertInvalid(
        r -> ((ObjectNode) table(r, 0).get("columns").get(1)).put("type", "FOO"),
        "unsupported type FOO");
    assertInvalid(
        r -> ((ObjectNode) table(r, 0).get("columns").get(1)).remove("type"), "unsupported type");
    assertInvalid(r -> ((ArrayNode) table(r, 0).get("primaryKey")).add("nope"), "primary key");
    assertInvalid(r -> table(r, 0).put("filePattern", "tables/x/*.avro"), "exactly one of");
    assertInvalid(r -> table(r, 0).remove("files"), "exactly one of");
    assertInvalid(r -> table(r, 0).putArray("files"), "exactly one of");
    assertInvalid(
        r -> ((ObjectNode) table(r, 0).get("files").get(0)).put("path", ""), "blank path");
    assertInvalid(
        r -> ((ObjectNode) r.get("source")).put("exportStartedAt", "yesterday"), "exportStartedAt");
  }

  @Test
  public void testTypesAreCaseInsensitive() throws Exception {
    GcsExportManifest m =
        GcsExportManifest.parse(
            mutate(
                r -> {
                  ((ObjectNode) table(r, 0).get("columns").get(3)).put("type", "json");
                  r.put("fileFormat", "avro");
                }));
    assertThat(m.getTables().get(0).getColumns().get(3).getType()).isEqualTo("json");
  }

  @Test
  public void testUnknownFieldsIgnored() throws Exception {
    GcsExportManifest m =
        GcsExportManifest.parse(
            mutate(
                r -> {
                  r.put("futureTopLevel", 42);
                  ((ObjectNode) r.get("source")).put("futureSource", "x");
                  table(r, 0).putObject("futureTable").put("a", 1);
                  ((ObjectNode) table(r, 0).get("columns").get(0)).put("futureColumn", true);
                  ((ObjectNode) table(r, 0).get("files").get(0)).put("futureFile", "y");
                }));
    assertThat(m.getTables()).hasSize(2);
  }

  @Test
  public void testReadTimeMicros() throws Exception {
    GcsExportManifest m = GcsExportManifest.parse(valid());
    long expected = Instant.parse("2026-10-17T10:00:00Z").toEpochMilli() * 1000L;
    assertThat(m.readTimeMicros()).isEqualTo(expected);

    GcsExportManifest fractional =
        GcsExportManifest.parse(
            mutate(
                r ->
                    ((ObjectNode) r.get("source"))
                        .put("exportStartedAt", "1970-01-01T00:00:01.000002Z")));
    assertThat(fractional.readTimeMicros()).isEqualTo(1_000_002L);

    GcsExportManifest noSource = GcsExportManifest.parse(mutate(r -> r.remove("source")));
    long before = System.currentTimeMillis() * 1000L;
    long actual = noSource.readTimeMicros();
    long after = System.currentTimeMillis() * 1000L;
    assertThat(actual).isAtLeast(before);
    assertThat(actual).isAtMost(after);
    assertThat(noSource.getSource()).isNull();
  }

  @Test
  public void testExportIdOrDefault() throws Exception {
    assertThat(GcsExportManifest.parse(valid()).exportIdOrDefault()).isEqualTo("20261017T100000Z");
    assertThat(GcsExportManifest.parse(mutate(r -> r.remove("exportId"))).exportIdOrDefault())
        .isEqualTo(GcsExportManifest.DEFAULT_EXPORT_ID);
    assertThat(GcsExportManifest.parse(mutate(r -> r.put("exportId", " "))).exportIdOrDefault())
        .isEqualTo("export");
  }
}
