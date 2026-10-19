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
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.ReaderImpl;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.source.gcs.iowrapper.GcsIoWrapper;
import com.google.cloud.teleport.v2.source.mysql.MySqlSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.templates.MigrateTableTransform;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Round trip: rows written by the template's own {@code gcsOutputDirectory} sink ({@link
 * MigrateTableTransform#writeToGCS}) are read back by the GCS source with identical payloads, shard
 * ids and primary keys. Proves that the GCS source reads exactly what the sink writes.
 */
@RunWith(JUnit4.class)
public class GcsBulkRoundTripTest {

  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private static SourceColumnType type(String name, Long... mods) {
    return new SourceColumnType(name, mods, null);
  }

  /**
   * A MySQL table covering primitive, logical (decimal, timestamp, date), record and JSON types.
   */
  private static SourceTableSchema ordersTable() {
    return SourceTableSchema.builder(
            new UnifiedTypeMapper(new MySqlSrcToSpSourceConnector().getTypeMapping()))
        .setTableName("orders")
        .setPrimaryKeyColumns(ImmutableList.of("id"))
        .addSourceColumnNameToSourceColumnType("id", type("BIGINT"))
        .addSourceColumnNameToSourceColumnType("name", type("VARCHAR", 20L))
        .addSourceColumnNameToSourceColumnType("qty", type("INTEGER"))
        .addSourceColumnNameToSourceColumnType("price", type("DECIMAL", 10L, 2L))
        .addSourceColumnNameToSourceColumnType("created", type("TIMESTAMP"))
        .addSourceColumnNameToSourceColumnType("updated", type("DATETIME"))
        .addSourceColumnNameToSourceColumnType("score", type("DOUBLE"))
        .addSourceColumnNameToSourceColumnType("flags", type("BIT"))
        .addSourceColumnNameToSourceColumnType("doc", type("JSON"))
        .build();
  }

  private static SourceTableSchema eventsTable() {
    return SourceTableSchema.builder(
            new UnifiedTypeMapper(new MySqlSrcToSpSourceConnector().getTypeMapping()))
        .setTableName("events")
        .setPrimaryKeyColumns(ImmutableList.of("k1", "k2"))
        .addSourceColumnNameToSourceColumnType("k1", type("VARCHAR", 10L))
        .addSourceColumnNameToSourceColumnType("k2", type("BIGINT"))
        .addSourceColumnNameToSourceColumnType("payload_json", type("JSON"))
        .build();
  }

  private static SourceRow order(SourceTableSchema schema, String shard, long id, boolean nulls) {
    SourceRow.Builder b =
        SourceRow.builder(GcsTestFixtures.REF, schema, shard, 1_700_000_000_000_000L)
            .setField("id", id);
    if (nulls) {
      return b.setField("name", null)
          .setField("qty", null)
          .setField("price", null)
          .setField("created", null)
          .setField("updated", null)
          .setField("score", null)
          .setField("flags", null)
          .setField("doc", null)
          .build();
    }
    GenericRecord dateTime =
        new GenericData.Record(
            schema.getAvroPayload().getField("updated").schema().getTypes().get(1));
    dateTime.put("date", 20_000 + (int) id);
    dateTime.put("time", 3_600_000_000L + id);
    return b.setField("name", "name-" + id + "-\u00e9")
        .setField("qty", (int) id * 3)
        .setField(
            "price",
            ByteBuffer.wrap(
                new BigDecimal("12345.6" + id).setScale(2).unscaledValue().toByteArray()))
        .setField("created", 1_760_000_000_123_456L + id)
        .setField("updated", dateTime)
        .setField("score", id + 0.25d)
        .setField("flags", 5L)
        .setField("doc", "{\"id\":" + id + ",\"tags\":[\"a\",1.5,null],\"n\":{\"x\":true}}")
        .build();
  }

  private static SourceRow event(SourceTableSchema schema, String k1, long k2) {
    return SourceRow.builder(GcsTestFixtures.REF, schema, null, 5L)
        .setField("k1", k1)
        .setField("k2", k2)
        .setField("payload_json", "[{\"k\":\"" + k1 + "\"}]")
        .build();
  }

  private static String key(SourceRow row) {
    GenericRecord p = row.getPayload();
    StringBuilder key = new StringBuilder(row.tableName());
    for (String column : row.primaryKeyColumns()) {
      key.append('|').append(p.get(column));
    }
    return key.toString();
  }

  @Test
  public void templateSinkOutputReadsBackIdentically() throws Exception {
    SourceTableSchema orders = ordersTable();
    SourceTableSchema events = eventsTable();
    List<SourceRow> written = new ArrayList<>();
    for (long id = 1; id <= 4; id++) {
      written.add(order(orders, id % 2 == 0 ? "shardA" : "shardB", id, false));
    }
    written.add(order(orders, "shardA", 5, true));
    written.add(event(events, "a", 1));
    written.add(event(events, "b", 2));

    // 1. Write with the template's own GCS sink.
    String root = tmp.getRoot().getAbsolutePath();
    TestPipeline writePipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect("MYSQL");
    options.setGcsOutputDirectory(root);
    MigrateTableTransform sink =
        new MigrateTableTransform(
            options,
            mock(SpannerConfig.class),
            mock(Ddl.class),
            mock(ISchemaMapper.class),
            mock(ReaderImpl.class));
    sink.writeToGCS(
        writePipeline.apply(Create.of(written).withCoder(SerializableCoder.of(SourceRow.class))),
        root);
    writePipeline.run().waitUntilFinish();
    assertThat(new java.io.File(root, "orders/shardA").isDirectory()).isTrue();
    assertThat(new java.io.File(root, "events").isDirectory()).isTrue();

    // 2. Read it back with the GCS source.
    GcsIoWrapper wrapper = new GcsIoWrapper(root, List.of(), null);
    Map<String, SourceTableSchema> readSchemas = new HashMap<>();
    wrapper
        .discoverTableSchema()
        .get(0)
        .tableSchemas()
        .forEach(s -> readSchemas.put(s.tableName(), s));
    assertThat(readSchemas.keySet()).containsExactly("orders", "events");
    // The discovered payload schemas are the written ones, field for field.
    assertThat(readSchemas.get("orders").getAvroPayload()).isEqualTo(orders.getAvroPayload());
    assertThat(readSchemas.get("events").getAvroPayload()).isEqualTo(events.getAvroPayload());
    assertThat(readSchemas.get("events").primaryKeyColumns()).containsExactly("k1", "k2");

    ReaderTransform readerTransform = ReaderImpl.of(wrapper).getReaderTransform();
    PCollectionTuple read = readPipeline.apply("Read", readerTransform.readTransform());
    PCollection<SourceRow> rows = read.get(readerTransform.sourceRowTag());

    Map<String, SourceRow> expected = new HashMap<>();
    for (SourceRow row : written) {
      expected.put(key(row), row);
    }
    PAssert.that(rows)
        .satisfies(
            actual -> {
              int count = 0;
              for (SourceRow row : actual) {
                count++;
                SourceRow want = expected.get(key(row));
                assertThat(want).isNotNull();
                assertThat(row.tableName()).isEqualTo(want.tableName());
                assertThat(row.shardId()).isEqualTo(want.shardId());
                assertThat(row.primaryKeyColumns()).isEqualTo(want.primaryKeyColumns());
                // GenericData.Record equality: same schema and same values.
                assertThat(row.getPayload()).isEqualTo(want.getPayload());
                assertThat(row.getPayload().toString()).isEqualTo(want.getPayload().toString());
                // Writing the read row again gives the same file schema.
                assertThat(row.gcsSchema().toString()).isEqualTo(want.gcsSchema().toString());
              }
              assertThat(count).isEqualTo(expected.size());
              return null;
            });
    readPipeline.run().waitUntilFinish();
  }
}
