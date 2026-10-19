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
package com.google.cloud.teleport.v2.source.gcs.reader;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsAvroRecordToSourceRowFn}. */
@RunWith(JUnit4.class)
public class GcsAvroRecordToSourceRowFnTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  /** Writes the rows in the bulk format and reads the records back (values become Avro types). */
  private List<GenericRecord> roundTrip(String shard, List<SourceRow> rows) throws Exception {
    File file = GcsTestFixtures.writeBulkFile(tmp.getRoot(), shard, 0, 1, rows);
    List<GenericRecord> out = new ArrayList<>();
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(file, new GenericDatumReader<GenericRecord>())) {
      while (reader.hasNext()) {
        out.add(reader.next());
      }
    }
    return out;
  }

  @Test
  public void testConvertsRecord() throws Exception {
    SourceTableSchema schema = GcsTestFixtures.edgeTable("knows");
    SourceRow original =
        GcsTestFixtures.edge(schema, "e1", "a/1", "b/2", "{\"w\":0.46245000000000003,\"n\":10}");
    GenericRecord record = roundTrip("shard7", List.of(original)).get(0);

    GcsAvroRecordToSourceRowFn fn =
        SerializableUtils.ensureSerializable(
            new GcsAvroRecordToSourceRowFn(GcsTestFixtures.REF, schema, 42L));
    SourceRow row = fn.apply(record);

    assertThat(row.tableName()).isEqualTo("knows");
    assertThat(row.shardId()).isEqualTo("shard7");
    assertThat(row.getReadTimeMicros()).isEqualTo(42L);
    assertThat(row.primaryKeyColumns()).containsExactly("key");
    assertThat(row.tableSchemaUUID()).isEqualTo(schema.tableSchemaUUID());
    GenericRecord payload = row.getPayload();
    assertThat(payload.get("key")).isInstanceOf(String.class);
    assertThat(payload.get("key")).isEqualTo("e1");
    assertThat(payload.get("from_id")).isEqualTo("a/1");
    assertThat(payload.get("to_id")).isEqualTo("b/2");
    // JSON numbers are canonicalized.
    assertThat(payload.get("doc")).isEqualTo("{\"w\":0.46245,\"n\":10}");
  }

  @Test
  public void testNullsAndMissingFields() throws Exception {
    SourceTableSchema docs = GcsTestFixtures.documentTable("docs");
    SourceRow original =
        SourceRow.builder(GcsTestFixtures.REF, docs, null, 1L)
            .setField("key", "k")
            .setField("id", null)
            .setField("rev", null)
            .setField("doc", null)
            .build();
    GenericRecord record = roundTrip(null, List.of(original)).get(0);
    // The table schema has an extra column that the files lack.
    SourceTableSchema edges = GcsTestFixtures.edgeTable("docs");
    SourceRow row = new GcsAvroRecordToSourceRowFn(GcsTestFixtures.REF, edges, 1L).apply(record);
    assertThat(row.shardId()).isNull();
    assertThat(row.getPayload().get("key")).isEqualTo("k");
    assertThat(row.getPayload().get("doc")).isNull();
    assertThat(row.getPayload().get("from_id")).isNull();
  }

  @Test
  public void testRecordWithoutPayloadFails() {
    Schema schema = SchemaBuilder.record("Other").fields().requiredString("key").endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("key", "k");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new GcsAvroRecordToSourceRowFn(
                        GcsTestFixtures.REF, GcsTestFixtures.documentTable("t"), 1L)
                    .apply(record));
    assertThat(e).hasMessageThat().contains("not in the bulk migration Avro format");
  }
}
