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
package com.google.cloud.teleport.v2.source.gcs;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.iowrapper.GcsIoWrapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Keeps the ArangoDB exporter's Avro schemas (resource files of {@code
 * tools/arangodb-gcs-exporter}) and the template's own GCS sink schema ({@link
 * SourceRow#gcsSchema()}) in lock-step: both must be byte-identical, also for tables discovered by
 * the GCS source from exporter-style files.
 */
@RunWith(JUnit4.class)
public class GcsGoldenSchemaTest {

  private static final Path EXPORTER_AVRO_DIR =
      Paths.get("tools", "arangodb-gcs-exporter", "src", "main", "resources", "avro");

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static String golden(String file) throws Exception {
    return new String(Files.readAllBytes(EXPORTER_AVRO_DIR.resolve(file)), StandardCharsets.UTF_8);
  }

  private static SourceRow documentRow(SourceTableSchema schema) {
    return SourceRow.builder(GcsTestFixtures.REF, schema, null, 1L)
        .setField("key", "k")
        .setField("id", "c/k")
        .setField("rev", "_r")
        .setField("doc", "{}")
        .build();
  }

  private static SourceRow edgeRow(SourceTableSchema schema) {
    return SourceRow.builder(GcsTestFixtures.REF, schema, null, 1L)
        .setField("key", "k")
        .setField("id", "c/k")
        .setField("rev", "_r")
        .setField("from_id", "a/1")
        .setField("to_id", "b/2")
        .setField("doc", "{}")
        .build();
  }

  @Test
  public void templateSinkSchemaEqualsExporterSchema() throws Exception {
    assertThat(documentRow(GcsTestFixtures.documentTable("users")).gcsSchema().toString())
        .isEqualTo(golden("arangodb-document.avsc"));
    assertThat(edgeRow(GcsTestFixtures.edgeTable("knows")).gcsSchema().toString())
        .isEqualTo(golden("arangodb-edge.avsc"));
  }

  @Test
  public void discoveredDocumentTableKeepsExporterSchema() throws Exception {
    String text = golden("arangodb-document.avsc");
    SourceTableSchema schema = discover(text, "users", false);
    assertThat(documentRow(schema).gcsSchema().toString()).isEqualTo(text);
  }

  @Test
  public void discoveredEdgeTableKeepsExporterSchema() throws Exception {
    String text = golden("arangodb-edge.avsc");
    SourceTableSchema schema = discover(text, "knows", true);
    assertThat(edgeRow(schema).gcsSchema().toString()).isEqualTo(text);
  }

  /**
   * Writes one exporter-style file ({@code <tableName>/<md5Hex(text)>-00000-of-00001.avro}, golden
   * schema, snappy) and returns the table schema the GCS source discovers from it.
   */
  private SourceTableSchema discover(String text, String table, boolean edge) throws Exception {
    Schema schema = new Schema.Parser().parse(text);
    // Avro keeps the unknown logical type "json" as a property, so the text survives parsing.
    assertThat(schema.toString()).isEqualTo(text);
    Schema payloadSchema = schema.getField("payload").schema();
    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("key", "k");
    payload.put("id", table + "/k");
    payload.put("rev", "_r");
    if (edge) {
      payload.put("from_id", "a/1");
      payload.put("to_id", "b/2");
    }
    payload.put("doc", "{\"a\":1}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("tableName", table);
    record.put("shardId", null);
    record.put("primaryKeys", List.of("key"));
    record.put("payload", payload);
    File root = tmp.newFolder("export");
    GcsTestFixtures.writeAvro(
        new File(root, table + "/" + DigestUtils.md5Hex(text) + "-00000-of-00001.avro"),
        schema,
        List.of(record));

    List<SourceTableSchema> tables =
        new GcsIoWrapper(root.getAbsolutePath(), List.of(), null)
            .discoverTableSchema()
            .get(0)
            .tableSchemas();
    assertThat(tables).hasSize(1);
    SourceTableSchema discovered = tables.get(0);
    assertThat(discovered.tableName()).isEqualTo(table);
    assertThat(discovered.primaryKeyColumns()).containsExactly("key");
    assertThat(discovered.getAvroPayload()).isEqualTo(payloadSchema);
    return discovered;
  }
}
