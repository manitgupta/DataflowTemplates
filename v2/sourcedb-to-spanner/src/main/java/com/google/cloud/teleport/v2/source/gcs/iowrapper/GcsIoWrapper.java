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
import com.google.cloud.teleport.v2.source.gcs.reader.GcsAvroTableReader;
import com.google.cloud.teleport.v2.source.gcs.schema.GcsSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link IoWrapper} for an Avro export in the bulk migration format (see {@link BulkGcsFormat}),
 * e.g. written by this template's {@code gcsOutputDirectory} or by the ArangoDB exporter. The
 * export is listed, and the selected tables' file headers read, once on the launcher in the
 * constructor; schemas and readers are cached.
 */
public final class GcsIoWrapper implements IoWrapper {

  private final SourceSchema sourceSchema;
  private final ImmutableMap<
          ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
      tableReaders;

  /**
   * Creates the IO wrapper.
   *
   * @param rootPath URI of the root directory of the export.
   * @param sourceTables tables to read; empty means all tables of the export.
   * @param waitOnSignal signal to wait on before reading, may be null.
   */
  public GcsIoWrapper(
      String rootPath, List<String> sourceTables, @Nullable Wait.OnSignal<?> waitOnSignal) {
    String root = BulkGcsFormat.stripTrailingSlash(rootPath);
    ImmutableSortedMap<String, ImmutableList<ResourceId>> tableFiles =
        GcsExportDiscovery.listTableFiles(root);
    SourceSchemaReference ref =
        SourceSchemaReference.ofGcs(
            GcsSchemaReference.builder().setExportId(exportId(root)).build());
    Instant now = Instant.now();
    long readTimeMicros =
        TimeUnit.SECONDS.toMicros(now.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(now.getNano());

    SourceSchema.Builder schemaBuilder = SourceSchema.builder().setSchemaReference(ref);
    ImmutableMap.Builder<
            ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        readers = ImmutableMap.builder();
    for (Map.Entry<String, String> entry :
        GcsIoWrapperHelper.selectTables(root, tableFiles.keySet(), sourceTables).entrySet()) {
      String exportTable = entry.getValue();
      GcsExportTable table =
          GcsExportDiscovery.describeTable(exportTable, tableFiles.get(exportTable));
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
              root, exportTable, ref, tableSchema, readTimeMicros, waitOnSignal));
    }
    this.sourceSchema = schemaBuilder.build();
    this.tableReaders = readers.build();
  }

  /** Returns the export id: the last path segment of the root directory. */
  static String exportId(String root) {
    String base = BulkGcsFormat.stripTrailingSlash(root);
    String id = base.substring(base.lastIndexOf('/') + 1);
    return id.isEmpty() ? base : id;
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
