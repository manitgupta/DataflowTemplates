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

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Converts a record of the bulk migration Avro format into a {@link SourceRow}: the shard id comes
 * from the record, the read time is the launch time, and payload fields are copied by name (see
 * {@link GcsMappingsProvider#toPayloadValue}).
 */
public final class GcsAvroRecordToSourceRowFn
    implements SerializableFunction<GenericRecord, SourceRow> {

  /** Prefix of the rows-read counter; the table name is appended. */
  static final String ROWS_READ_COUNTER_PREFIX = "gcs_source_rows_read_";

  private final SourceSchemaReference ref;
  private final SourceTableSchema tableSchema;
  private final long readTimeMicros;
  private transient Counter rowsRead;

  /**
   * Creates the function.
   *
   * @param ref schema reference of the export.
   * @param tableSchema schema of the table.
   * @param readTimeMicros read timestamp of every row.
   */
  public GcsAvroRecordToSourceRowFn(
      SourceSchemaReference ref, SourceTableSchema tableSchema, long readTimeMicros) {
    this.ref = ref;
    this.tableSchema = tableSchema;
    this.readTimeMicros = readTimeMicros;
  }

  @Override
  public SourceRow apply(GenericRecord record) {
    Object payloadValue =
        record.hasField(BulkGcsFormat.PAYLOAD_FIELD)
            ? record.get(BulkGcsFormat.PAYLOAD_FIELD)
            : null;
    if (!(payloadValue instanceof GenericRecord)) {
      throw new IllegalArgumentException(
          "Table "
              + tableSchema.tableName()
              + ": record without a "
              + BulkGcsFormat.PAYLOAD_FIELD
              + " record; not in the bulk migration Avro format: "
              + abbreviate(String.valueOf(record)));
    }
    GenericRecord payload = (GenericRecord) payloadValue;
    Object shardId =
        record.hasField(BulkGcsFormat.SHARD_ID_FIELD)
            ? record.get(BulkGcsFormat.SHARD_ID_FIELD)
            : null;
    SourceRow.Builder builder =
        SourceRow.builder(
            ref, tableSchema, shardId == null ? null : shardId.toString(), readTimeMicros);
    for (Schema.Field field : tableSchema.getAvroPayload().getFields()) {
      String name = field.name();
      Object value = payload.hasField(name) ? payload.get(name) : null;
      try {
        builder.setField(name, GcsMappingsProvider.toPayloadValue(field.schema(), value));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            "Table "
                + tableSchema.tableName()
                + " column "
                + name
                + " key="
                + primaryKey(payload)
                + ": "
                + e.getMessage(),
            e);
      }
    }
    if (rowsRead == null) {
      rowsRead =
          Metrics.counter(
              GcsAvroRecordToSourceRowFn.class, ROWS_READ_COUNTER_PREFIX + tableSchema.tableName());
    }
    rowsRead.inc();
    return builder.build();
  }

  private List<Object> primaryKey(GenericRecord payload) {
    List<Object> key = new ArrayList<>();
    for (String column : tableSchema.primaryKeyColumns()) {
      key.add(payload.hasField(column) ? payload.get(column) : null);
    }
    return key;
  }

  private static String abbreviate(String s) {
    return s.length() > 200 ? s.substring(0, 200) + "..." : s;
  }
}
