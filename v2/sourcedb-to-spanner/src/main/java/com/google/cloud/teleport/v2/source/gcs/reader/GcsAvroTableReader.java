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
import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsAvroReader;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Reads the Avro files of one table of a bulk migration Avro export ({@code
 * root/<tableName>/**.avro}) with the shared {@link BulkGcsAvroReader} and converts the records
 * into {@link SourceRow}s.
 */
public final class GcsAvroTableReader extends PTransform<PBegin, PCollection<SourceRow>> {

  private final String root;
  private final String exportTable;
  private final SourceSchemaReference ref;
  private final SourceTableSchema tableSchema;
  private final long readTimeMicros;
  @Nullable private final Wait.OnSignal<?> waitOnSignal;

  /**
   * Creates the reader.
   *
   * @param root root directory of the export.
   * @param exportTable name of the table's directory under {@code root}.
   * @param ref schema reference of the export.
   * @param tableSchema schema of the table.
   * @param readTimeMicros read timestamp of every row.
   * @param waitOnSignal signal to wait on before reading, may be null.
   */
  public GcsAvroTableReader(
      String root,
      String exportTable,
      SourceSchemaReference ref,
      SourceTableSchema tableSchema,
      long readTimeMicros,
      @Nullable Wait.OnSignal<?> waitOnSignal) {
    this.root = root;
    this.exportTable = exportTable;
    this.ref = ref;
    this.tableSchema = tableSchema;
    this.readTimeMicros = readTimeMicros;
    this.waitOnSignal = waitOnSignal;
  }

  @Override
  public PCollection<SourceRow> expand(PBegin input) {
    return input
        .apply(
            "ReadAvroRecords",
            BulkGcsAvroReader.of(root, List.of(exportTable), List.of())
                .withWaitOnSignal(waitOnSignal))
        .apply(
            "ToSourceRow",
            MapElements.into(TypeDescriptor.of(SourceRow.class))
                .via(new GcsAvroRecordToSourceRowFn(ref, tableSchema, readTimeMicros)))
        .setCoder(SourceRowCoders.forPipeline(input.getPipeline()));
  }
}
