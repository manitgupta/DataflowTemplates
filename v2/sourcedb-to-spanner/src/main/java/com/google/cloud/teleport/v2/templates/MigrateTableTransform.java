/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.ReaderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.transformer.SourceRowToTableRowDoFn;
import com.google.cloud.teleport.v2.writer.BQWriter;
import com.google.cloud.teleport.v2.writer.DeadLetterQueue;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateTableTransform extends PTransform<PBegin, PCollection<Void>> {
  private static final Logger LOG = LoggerFactory.getLogger(MigrateTableTransform.class);

  private transient SourceDbToSpannerOptions options;
  private SpannerConfig spannerConfig;
  private Ddl ddl;
  private ISchemaMapper schemaMapper;
  private ReaderImpl reader;
  private String shardId;
  private SQLDialect sqlDialect;

  private Map<String, String> srcTableToShardIdColumnMap;
  private PCollectionView<Map<String, String>> bqSchemaView;

  public MigrateTableTransform(
      SourceDbToSpannerOptions options,
      SpannerConfig spannerConfig,
      Ddl ddl,
      ISchemaMapper schemaMapper,
      ReaderImpl reader,
      String shardId,
      Map<String, String> srcTableToShardIdColumnMap,
      PCollectionView<Map<String, String>> bqSchemaView) {
    this.options = options;
    this.spannerConfig = spannerConfig;
    this.ddl = ddl;
    this.schemaMapper = schemaMapper;
    this.reader = reader;
    this.shardId = StringUtils.isEmpty(shardId) ? "" : shardId;
    this.sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());
    this.srcTableToShardIdColumnMap = srcTableToShardIdColumnMap;
    this.bqSchemaView = bqSchemaView;
  }

  @Override
  public PCollection<Void> expand(PBegin input) {
    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple rowsAndTables = input.apply("Read_rows", readerTransform.readTransform());
    PCollection<SourceRow> sourceRows = rowsAndTables.get(readerTransform.sourceRowTag());

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();

    // Transform source data to Spanner Compatible Data
    SourceRowToTableRowDoFn transformDoFn =
        SourceRowToTableRowDoFn.create(
            schemaMapper, customTransformation);
    PCollectionTuple transformationResult =
        sourceRows.apply(
            "Transform",
            ParDo.of(transformDoFn)
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(
                        Arrays.asList(
                            SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR,
                            SourceDbToSpannerConstants.FILTERED_EVENT_TAG))));

    // Write to BQ
    String projectId = options.getProjectId();
    String databaseId = options.getDatabaseId();
    BQWriter bqWriter = new BQWriter(projectId, databaseId, bqSchemaView, schemaMapper);
    WriteResult writeResult = bqWriter.writeToBQ(
        transformationResult
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
            .setCoder(SerializableCoder.of(RowContext.class)));

    String outputDirectory = options.getOutputDirectory();
    if (!outputDirectory.endsWith("/")) {
      outputDirectory += "/";
    }

    /*
     * Write filtered records to GCS
     */
    String filterEventsDirectory = outputDirectory + "filteredEvents/" + shardId;
    LOG.info("Filtered events directory: {}", filterEventsDirectory);
    DeadLetterQueue filteredEventsQueue =
        DeadLetterQueue.create(
            filterEventsDirectory, ddl, srcTableToShardIdColumnMap, sqlDialect, this.schemaMapper);
    filteredEventsQueue.filteredEventsToDLQ(
        transformationResult
            .get(SourceDbToSpannerConstants.FILTERED_EVENT_TAG)
            .setCoder(SerializableCoder.of(RowContext.class)));

    PCollection<TableDestination> successfulTableLoads = writeResult.getSuccessfulTableLoads();

    return successfulTableLoads.apply(
        "LogSuccessfulLoads",
        ParDo.of(new DoFn<TableDestination, Void>() {
          @ProcessElement
          public void processElement(@Element TableDestination destination, ProcessContext c) {
            LOG.info(
                "✅ Successfully loaded tables at timestamp: {}. Destination: {}",
                c.timestamp(),
                destination.getTableSpec()
            );
          }
        })
    );
  }
}
