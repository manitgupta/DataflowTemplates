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

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsExportManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsTableManifest;
import com.google.cloud.teleport.v2.source.gcs.reader.GcsAvroTableReader;
import com.google.cloud.teleport.v2.source.gcs.schema.GcsSchemaReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IoWrapper} for a GCS file export. The manifest is read once, on the launcher, in the
 * constructor; schemas and readers are cached.
 */
public final class GcsIoWrapper implements IoWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIoWrapper.class);

  private final SourceSchema sourceSchema;
  private final ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;

  /**
   * Creates the IO wrapper.
   *
   * @param manifestPath URI of the export manifest.
   * @param sourceTables tables to read; empty means all tables of the manifest.
   * @param waitOnSignal signal to wait on before reading, may be null.
   */
  public GcsIoWrapper(
      String manifestPath, List<String> sourceTables, @Nullable Wait.OnSignal<?> waitOnSignal) {
    GcsExportManifest manifest = GcsExportManifest.read(manifestPath);
    if (manifest.getSource() != null
        && Boolean.FALSE.equals(manifest.getSource().getConsistent())) {
      LOG.warn(
          "Export {} at {} is marked as not consistent: the source changed during the export.",
          manifest.exportIdOrDefault(),
          manifestPath);
    }
    SourceSchemaReference ref =
        SourceSchemaReference.ofGcs(
            GcsSchemaReference.builder().setExportId(manifest.exportIdOrDefault()).build());
    long readTimeMicros = manifest.readTimeMicros();

    SourceSchema.Builder schemaBuilder = SourceSchema.builder().setSchemaReference(ref);
    ImmutableMap.Builder<
            ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        readers = ImmutableMap.builder();
    for (Map.Entry<String, GcsTableManifest> entry :
        GcsIoWrapperHelper.selectTables(manifest, sourceTables).entrySet()) {
      GcsTableManifest table = entry.getValue();
      SourceTableSchema tableSchema = GcsIoWrapperHelper.buildTableSchema(entry.getKey(), table);
      schemaBuilder.addTableSchema(tableSchema);
      SourceTableReference tableRef =
          SourceTableReference.builder()
              .setSourceSchemaReference(ref)
              .setSourceTableSchemaUUID(tableSchema.tableSchemaUUID())
              .setSourceTableName(tableSchema.tableName())
              .build();
      readers.put(
          ImmutableList.of(tableRef),
          new GcsAvroTableReader(
              table.resolveFilePatterns(manifestPath),
              ref,
              tableSchema,
              GcsIoWrapperHelper.columnTypes(table),
              readTimeMicros,
              table.getRowCount(),
              waitOnSignal));
    }
    this.sourceSchema = schemaBuilder.build();
    this.tableReaders = readers.build();
  }

  /** Get the reader transforms, one per selected table. */
  @Override
  public ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      getTableReaders() {
    return tableReaders;
  }

  /** Returns the schema of the selected tables. */
  @Override
  public ImmutableList<SourceSchema> discoverTableSchema() {
    return ImmutableList.of(sourceSchema);
  }
}
