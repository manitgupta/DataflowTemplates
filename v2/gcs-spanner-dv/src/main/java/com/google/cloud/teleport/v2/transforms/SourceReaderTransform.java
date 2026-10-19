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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.dofn.SourceHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsAvroReader;
import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;

/**
 * Reads the source Avro files under {@code gcsInputDirectory} and hashes their records.
 *
 * <p>Bulk migration output is laid out as root/table/[shardId/]file.avro and read with the shared
 * {@link BulkGcsAvroReader}; see {@link BulkGcsFormat} for which files get listed for the
 * configured table and shard filters.
 */
public class SourceReaderTransform
    extends PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private final String gcsInputDirectory;
  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;
  private final CustomTransformation customTransformation;
  private final TableConfiguration tableConfig;

  public SourceReaderTransform(
      String gcsInputDirectory,
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider,
      CustomTransformation customTransformation,
      TableConfiguration tableConfig) {
    this.gcsInputDirectory = gcsInputDirectory;
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
    this.customTransformation = customTransformation;
    this.tableConfig = tableConfig;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin input) {
    return input
        .apply(
            "ReadSourceAvroRecords",
            BulkGcsAvroReader.of(
                gcsInputDirectory, sourceTables(tableConfig), shardIds(tableConfig)))
        .apply(
            "CalculateSourceRecordsHash",
            ParDo.of(new SourceHashFn(ddlView, schemaMapperProvider, customTransformation))
                .withSideInputs(ddlView));
  }

  /** Delegates to {@link BulkGcsFormat#filePatterns}. */
  static List<String> getFilePatterns(String gcsInputDirectory, TableConfiguration tableConfig) {
    return BulkGcsFormat.filePatterns(
        gcsInputDirectory, sourceTables(tableConfig), shardIds(tableConfig));
  }

  /** Delegates to {@link BulkGcsFormat#isConfiguredFile}. */
  static boolean isConfiguredFile(String root, TableConfiguration tableConfig, String path) {
    return BulkGcsFormat.isConfiguredFile(
        root, sourceTables(tableConfig), shardIds(tableConfig), path);
  }

  private static Set<String> sourceTables(TableConfiguration tableConfig) {
    return tableConfig == null ? Set.of() : tableConfig.getSourceTables();
  }

  private static Set<String> shardIds(TableConfiguration tableConfig) {
    return tableConfig == null ? Set.of() : tableConfig.getShardIds();
  }
}
