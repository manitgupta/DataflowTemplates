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
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Converts an Avro record of a GCS export file into a {@link SourceRow}. */
public final class GcsAvroRecordToSourceRowFn
    implements SerializableFunction<GenericRecord, SourceRow> {

  /** Prefix of the rows-read counter; the table name is appended. */
  static final String ROWS_READ_COUNTER_PREFIX = "gcs_source_rows_read_";

  private static final String KEY_FIELD = "key";

  private final SourceSchemaReference ref;
  private final SourceTableSchema tableSchema;
  private final ImmutableMap<String, String> columnTypes;
  private final long readTimeMicros;
  private transient Counter rowsRead;

  /**
   * Creates the function.
   *
   * @param ref schema reference of the export.
   * @param tableSchema schema of the table.
   * @param columnTypes column name to manifest type, in manifest order.
   * @param readTimeMicros read timestamp of every row.
   */
  public GcsAvroRecordToSourceRowFn(
      SourceSchemaReference ref,
      SourceTableSchema tableSchema,
      ImmutableMap<String, String> columnTypes,
      long readTimeMicros) {
    this.ref = ref;
    this.tableSchema = tableSchema;
    this.columnTypes = columnTypes;
    this.readTimeMicros = readTimeMicros;
  }

  @Override
  public SourceRow apply(GenericRecord record) {
    SourceRow.Builder builder =
        SourceRow.builder(ref, tableSchema, /* shardId= */ null, readTimeMicros);
    for (Map.Entry<String, String> column : columnTypes.entrySet()) {
      String name = column.getKey();
      Object value = record.hasField(name) ? record.get(name) : null;
      try {
        builder.setField(name, GcsMappingsProvider.toPayloadValue(column.getValue(), value));
      } catch (IllegalArgumentException e) {
        Object key = record.hasField(KEY_FIELD) ? record.get(KEY_FIELD) : null;
        throw new IllegalArgumentException(
            "Table "
                + tableSchema.tableName()
                + " column "
                + name
                + " key="
                + key
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
}
