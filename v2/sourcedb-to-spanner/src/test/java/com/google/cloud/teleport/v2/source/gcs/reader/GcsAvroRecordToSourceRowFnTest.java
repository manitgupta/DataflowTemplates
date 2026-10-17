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
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.cloud.teleport.v2.source.gcs.schema.GcsSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

  static final SourceSchemaReference REF =
      SourceSchemaReference.ofGcs(GcsSchemaReference.builder().setExportId("e").build());

  static SourceTableSchema tableSchema(String table, Map<String, String> columns) {
    SourceTableSchema.Builder b =
        SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
            .setTableName(table)
            .setPrimaryKeyColumns(ImmutableList.of("key"));
    columns.forEach(
        (n, t) ->
            b.addSourceColumnNameToSourceColumnType(
                n, new SourceColumnType(t, new Long[] {}, new Long[] {})));
    return b.build();
  }

  static final ImmutableMap<String, String> DOC_COLUMNS =
      ImmutableMap.of("key", "STRING", "id", "STRING", "rev", "STRING", "doc", "JSON");

  static final ImmutableMap<String, String> EDGE_COLUMNS =
      ImmutableMap.<String, String>builder()
          .put("key", "STRING")
          .put("id", "STRING")
          .put("rev", "STRING")
          .put("from_id", "STRING")
          .put("to_id", "STRING")
          .put("doc", "JSON")
          .build();

  @Test
  public void testDocumentRecord() throws Exception {
    SourceTableSchema schema = tableSchema("user_profiles", DOC_COLUMNS);
    GcsAvroRecordToSourceRowFn fn =
        SerializableUtils.clone(new GcsAvroRecordToSourceRowFn(REF, schema, DOC_COLUMNS, 42L));
    GenericRecord record =
        GcsTestFixtures.roundTrip(
                tmp.getRoot(),
                GcsTestFixtures.DOCUMENT_SCHEMA,
                List.of(GcsTestFixtures.document("u1", "{\"_key\":\"u1\",\"n\":1.50}")))
            .get(0);

    SourceRow row = fn.apply(record);

    assertThat(row.tableName()).isEqualTo("user_profiles");
    assertThat(row.tableSchemaUUID()).isEqualTo(schema.tableSchemaUUID());
    assertThat(row.primaryKeyColumns()).containsExactly("key");
    assertThat(row.shardId()).isNull();
    assertThat(row.getReadTimeMicros()).isEqualTo(42L);
    assertThat(row.sourceSchemaReference()).isEqualTo(REF);
    GenericRecord payload = row.getPayload();
    assertThat(payload.get("key")).isEqualTo("u1");
    assertThat(payload.get("id")).isEqualTo("user_profiles/u1");
    assertThat(payload.get("rev")).isEqualTo("_revu1");
    assertThat(payload.get("doc")).isEqualTo("{\"_key\":\"u1\",\"n\":1.50}");
    Schema docSchema = payload.getSchema().getField("doc").schema().getTypes().get(1);
    assertThat(docSchema.getProp("logicalType")).isEqualTo("json");
  }

  @Test
  public void testEdgeRecord() throws Exception {
    SourceTableSchema schema = tableSchema("knows", EDGE_COLUMNS);
    GcsAvroRecordToSourceRowFn fn = new GcsAvroRecordToSourceRowFn(REF, schema, EDGE_COLUMNS, 7L);
    GenericRecord record =
        GcsTestFixtures.roundTrip(
                tmp.getRoot(),
                GcsTestFixtures.EDGE_SCHEMA,
                List.of(GcsTestFixtures.edge("e1", "a/1", "b/2", "{}")))
            .get(0);
    GenericRecord payload = fn.apply(record).getPayload();
    assertThat(payload.get("from_id")).isEqualTo("a/1");
    assertThat(payload.get("to_id")).isEqualTo("b/2");
    assertThat(payload.get("doc")).isEqualTo("{}");
  }

  @Test
  public void testUnionOrdersAndMissingField() throws Exception {
    Schema stringFirst =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"key\",\"type\":[\"string\",\"null\"]},"
                    + "{\"name\":\"doc\",\"type\":[\"null\",\"string\"]}]}");
    GenericRecord r1 = new GenericData.Record(stringFirst);
    r1.put("key", "k1");
    r1.put("doc", "[1]");
    GenericRecord r2 = new GenericData.Record(stringFirst);
    r2.put("key", "k2");
    r2.put("doc", null);
    List<GenericRecord> records =
        GcsTestFixtures.roundTrip(tmp.getRoot(), stringFirst, List.of(r1, r2));

    SourceTableSchema schema = tableSchema("t", DOC_COLUMNS);
    GcsAvroRecordToSourceRowFn fn = new GcsAvroRecordToSourceRowFn(REF, schema, DOC_COLUMNS, 1L);
    GenericRecord p1 = fn.apply(records.get(0)).getPayload();
    assertThat(p1.get("key")).isEqualTo("k1");
    assertThat(p1.get("doc")).isEqualTo("[1]");
    // id and rev are not in the file.
    assertThat(p1.get("id")).isNull();
    assertThat(p1.get("rev")).isNull();
    GenericRecord p2 = fn.apply(records.get(1)).getPayload();
    assertThat(p2.get("doc")).isNull();
  }

  @Test
  public void testTypedConversionFailureMessage() throws Exception {
    Schema s =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"key\",\"type\":\"string\"},"
                    + "{\"name\":\"age\",\"type\":\"string\"}]}");
    GenericRecord r = new GenericData.Record(s);
    r.put("key", "k9");
    r.put("age", "old");
    ImmutableMap<String, String> cols = ImmutableMap.of("key", "STRING", "age", "LONG");
    GcsAvroRecordToSourceRowFn fn =
        new GcsAvroRecordToSourceRowFn(REF, tableSchema("people", cols), cols, 1L);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fn.apply(r));
    assertThat(e).hasMessageThat().contains("Table people");
    assertThat(e).hasMessageThat().contains("column age");
    assertThat(e).hasMessageThat().contains("key=k9");

    r.put("age", "41");
    assertThat(fn.apply(r).getPayload().get("age")).isEqualTo(41L);
  }

  @Test
  public void testTypedConversionFailureWithoutKeyField() {
    Schema s =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"age\",\"type\":\"string\"}]}");
    GenericRecord r = new GenericData.Record(s);
    r.put("age", "old");
    ImmutableMap<String, String> cols = ImmutableMap.of("age", "LONG");
    SourceTableSchema schema =
        SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
            .setTableName("people")
            .addSourceColumnNameToSourceColumnType(
                "age", new SourceColumnType("LONG", new Long[] {}, new Long[] {}))
            .build();
    GcsAvroRecordToSourceRowFn fn = new GcsAvroRecordToSourceRowFn(REF, schema, cols, 1L);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fn.apply(r));
    assertThat(e).hasMessageThat().contains("key=null");
  }
}
