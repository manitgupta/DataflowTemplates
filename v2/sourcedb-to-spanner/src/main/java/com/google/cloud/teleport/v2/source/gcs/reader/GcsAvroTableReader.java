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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads the Avro files of one table of a GCS export into {@link SourceRow}s. Every listed file (or
 * the file pattern) must match at least one file, otherwise the pipeline fails.
 */
public final class GcsAvroTableReader extends PTransform<PBegin, PCollection<SourceRow>> {

  private final ImmutableList<String> filePatterns;
  private final SourceSchemaReference ref;
  private final SourceTableSchema tableSchema;
  private final ImmutableMap<String, String> columnTypes;
  private final long readTimeMicros;
  @Nullable private final Long expectedRowCount;
  @Nullable private final Wait.OnSignal<?> waitOnSignal;

  /**
   * Creates the reader.
   *
   * @param filePatterns resolved file URIs (or globs) of the table.
   * @param ref schema reference of the export.
   * @param tableSchema schema of the table.
   * @param columnTypes column name to manifest type, in manifest order.
   * @param readTimeMicros read timestamp of every row.
   * @param expectedRowCount manifest row count; enables the row count check when non-null.
   * @param waitOnSignal signal to wait on before reading, may be null.
   */
  public GcsAvroTableReader(
      List<String> filePatterns,
      SourceSchemaReference ref,
      SourceTableSchema tableSchema,
      ImmutableMap<String, String> columnTypes,
      long readTimeMicros,
      @Nullable Long expectedRowCount,
      @Nullable Wait.OnSignal<?> waitOnSignal) {
    this.filePatterns = ImmutableList.copyOf(filePatterns);
    this.ref = ref;
    this.tableSchema = tableSchema;
    this.columnTypes = columnTypes;
    this.readTimeMicros = readTimeMicros;
    this.expectedRowCount = expectedRowCount;
    this.waitOnSignal = waitOnSignal;
  }

  @Override
  public PCollection<SourceRow> expand(PBegin input) {
    PCollection<String> seed =
        input.apply("CreateFilePatterns", Create.of(filePatterns).withCoder(StringUtf8Coder.of()));
    if (waitOnSignal != null) {
      seed = waitOn(seed, waitOnSignal);
    }
    // Create.of is a single bundle: spread the (possibly thousands of) file specs over workers so
    // that their GCS metadata lookups in MatchFiles run in parallel instead of one after another.
    PCollection<SourceRow> rows =
        seed.apply("SpreadFilePatterns", Reshuffle.viaRandomKey())
            .apply(
                "MatchFiles",
                FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            .apply(
                "ReadMatches",
                FileIO.readMatches()
                    .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.PROHIBIT))
            .apply(
                "ParseToSourceRow",
                AvroIO.parseFilesGenericRecords(
                        new GcsAvroRecordToSourceRowFn(
                            ref, tableSchema, columnTypes, readTimeMicros))
                    .withCoder(SourceRowCoders.forPipeline(input.getPipeline())));
    if (expectedRowCount != null) {
      rows.apply("CountRows", Count.globally())
          .apply(
              "CheckRowCount",
              ParDo.of(new ExpectedRowCountCheckFn(tableSchema.tableName(), expectedRowCount)));
    }
    return rows;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PCollection<String> waitOn(PCollection<String> seed, Wait.OnSignal<?> signal) {
    // Wait.OnSignal is typed on the signal's element type; it passes any element type through.
    return (PCollection<String>)
        ((PCollection) seed).apply("WaitOnPreviousLevel", (PTransform) signal);
  }
}
