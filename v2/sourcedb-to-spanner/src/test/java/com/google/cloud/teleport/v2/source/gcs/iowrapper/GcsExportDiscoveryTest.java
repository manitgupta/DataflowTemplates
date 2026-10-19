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
package com.google.cloud.teleport.v2.source.gcs.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsExportDiscovery}. */
@RunWith(JUnit4.class)
public class GcsExportDiscoveryTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static List<String> names(List<ResourceId> files) {
    return files.stream().map(ResourceId::getFilename).collect(Collectors.toList());
  }

  @Test
  public void listsTablesAtAnyDepthAndIgnoresOtherFiles() throws Exception {
    File root = tmp.getRoot();
    SourceTableSchema users = GcsTestFixtures.documentTable("users");
    GcsTestFixtures.writeBulkFile(
        root, "shard_b", 0, 1, List.of(GcsTestFixtures.document(users, "u2", "{}")));
    GcsTestFixtures.writeBulkFile(
        root, "shard_a", 0, 1, List.of(GcsTestFixtures.document(users, "u1", "{}")));
    GcsTestFixtures.writeBulkFile(
        root,
        null,
        0,
        1,
        List.of(GcsTestFixtures.edge(GcsTestFixtures.edgeTable("knows"), "e", "a", "b", "{}")));
    // Not table data: a file directly under root, and non-Avro files.
    GcsTestFixtures.writeAvro(
        new File(root, "top.avro"), users.avroSchema(), List.<GenericRecord>of());
    new File(root, "arangodb-export.json").createNewFile();
    new File(root, "users/notes.txt").createNewFile();
    new File(root, "empty_dir").mkdirs();

    ImmutableSortedMap<String, ImmutableList<ResourceId>> tables =
        GcsExportDiscovery.listTableFiles(root.getAbsolutePath() + "/");
    assertThat(tables.keySet()).containsExactly("knows", "users").inOrder();
    assertThat(tables.get("users")).hasSize(2);
    assertThat(tables.get("users").get(0).toString()).contains("/users/shard_a/");
    assertThat(tables.get("users").get(1).toString()).contains("/users/shard_b/");
  }

  @Test
  public void emptyOrMissingRootHasNoTables() {
    assertThat(GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath())).isEmpty();
    assertThat(GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath() + "/missing"))
        .isEmpty();
  }

  @Test
  public void describesTableFromHeaderAndFirstRecord() throws Exception {
    File root = tmp.getRoot();
    SourceTableSchema knows = GcsTestFixtures.edgeTable("knows");
    SourceRow row = GcsTestFixtures.edge(knows, "e1", "a/1", "b/2", "{}");
    GcsTestFixtures.writeBulkFile(root, null, 0, 2, List.of(row));
    GcsTestFixtures.writeBulkFile(root, null, 1, 2, List.of(row));

    ImmutableList<ResourceId> files =
        GcsExportDiscovery.listTableFiles(root.getAbsolutePath()).get("knows");
    assertThat(files).hasSize(2);
    GcsExportTable table = GcsExportDiscovery.describeTable("knows", files);
    assertThat(table.name()).isEqualTo("knows");
    assertThat(table.primaryKeys()).containsExactly("key");
    assertThat(table.payloadSchema()).isEqualTo(knows.getAvroPayload());
  }

  @Test
  public void emptyFilesGiveNoPrimaryKeys() throws Exception {
    SourceTableSchema users = GcsTestFixtures.documentTable("users");
    Schema schema = GcsTestFixtures.document(users, "u", "{}").gcsSchema();
    GcsTestFixtures.writeAvro(
        new File(tmp.getRoot(), "users/x-00000-of-00001.avro"), schema, List.of());
    GcsExportTable table =
        GcsExportDiscovery.describeTable(
            "users",
            GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath()).get("users"));
    assertThat(table.primaryKeys()).isEmpty();
    assertThat(table.payloadSchema()).isEqualTo(users.getAvroPayload());
  }

  @Test
  public void primaryKeysFromLaterFileWhenFirstIsEmpty() throws Exception {
    SourceTableSchema users = GcsTestFixtures.documentTable("users");
    SourceRow row = GcsTestFixtures.document(users, "u", "{}");
    String md5 =
        GcsTestFixtures.writeBulkFile(tmp.getRoot(), null, 1, 2, List.of(row))
            .getName()
            .substring(0, 32);
    GcsTestFixtures.writeAvro(
        new File(tmp.getRoot(), "users/" + md5 + "-00000-of-00002.avro"),
        row.gcsSchema(),
        List.of());
    ImmutableList<ResourceId> files =
        GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath()).get("users");
    assertThat(names(files).get(0)).endsWith("-00000-of-00002.avro");
    assertThat(GcsExportDiscovery.describeTable("users", files).primaryKeys())
        .containsExactly("key");
  }

  @Test
  public void differentPayloadSchemasFail() throws Exception {
    File root = tmp.getRoot();
    GcsTestFixtures.writeBulkFile(
        root,
        null,
        0,
        1,
        List.of(GcsTestFixtures.document(GcsTestFixtures.documentTable("t"), "u", "{}")));
    GcsTestFixtures.writeBulkFile(
        root,
        null,
        0,
        1,
        List.of(GcsTestFixtures.edge(GcsTestFixtures.edgeTable("t"), "e", "a", "b", "{}")));
    ImmutableList<ResourceId> files =
        GcsExportDiscovery.listTableFiles(root.getAbsolutePath()).get("t");
    assertThat(files).hasSize(2);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> GcsExportDiscovery.describeTable("t", files));
    assertThat(e).hasMessageThat().contains("different payload schemas");
  }

  @Test
  public void sameSchemaWithDifferentPrefixesIsAccepted() throws Exception {
    SourceTableSchema users = GcsTestFixtures.documentTable("users");
    SourceRow row = GcsTestFixtures.document(users, "u", "{}");
    GcsTestFixtures.writeAvro(
        new File(tmp.getRoot(), "users/aaa-00000-of-00001.avro"),
        row.gcsSchema(),
        List.of(row.toGcsRecord()));
    GcsTestFixtures.writeAvro(
        new File(tmp.getRoot(), "users/bbb-00000-of-00001.avro"),
        row.gcsSchema(),
        List.of(row.toGcsRecord()));
    assertThat(
            GcsExportDiscovery.describeTable(
                    "users",
                    GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath()).get("users"))
                .payloadSchema())
        .isEqualTo(users.getAvroPayload());
  }

  @Test
  public void nonBulkFileFails() throws Exception {
    Schema schema = SchemaBuilder.record("Other").fields().requiredString("key").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("key", "k");
    GcsTestFixtures.writeAvro(new File(tmp.getRoot(), "t/part-0.avro"), schema, List.of(record));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GcsExportDiscovery.describeTable(
                    "t",
                    GcsExportDiscovery.listTableFiles(tmp.getRoot().getAbsolutePath()).get("t")));
    assertThat(e).hasMessageThat().contains("not in the bulk migration Avro format");
  }

  @Test
  public void noFilesFails() {
    assertThrows(
        IllegalArgumentException.class, () -> GcsExportDiscovery.describeTable("t", List.of()));
  }

  @Test
  public void schemaPrefix() {
    assertThat(GcsExportDiscovery.schemaPrefix("abc-00000-of-00001.avro")).isEqualTo("abc");
    assertThat(GcsExportDiscovery.schemaPrefix("abc.avro")).isEqualTo("abc.avro");
    assertThat(GcsExportDiscovery.schemaPrefix("-x.avro")).isEqualTo("-x.avro");
  }
}
