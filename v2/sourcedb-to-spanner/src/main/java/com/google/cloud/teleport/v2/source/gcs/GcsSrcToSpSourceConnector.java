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

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.ISrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.gcs.iowrapper.GcsIoWrapperFactory;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.templates.DbConfigContainerDefaultImpl;
import com.google.cloud.teleport.v2.templates.PipelineController;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/**
 * Source connector for Avro exports on GCS in the bulk migration format (see {@code
 * com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat}), e.g. written by this
 * template's {@code gcsOutputDirectory} or by the ArangoDB Spark exporter.
 */
public class GcsSrcToSpSourceConnector implements ISrcToSpSourceConnector {

  /** Creates the connector. Performs no I/O, since the connector may be constructed repeatedly. */
  public GcsSrcToSpSourceConnector() {}

  @Override
  public String getSourceType() {
    return Constants.GCS_SOURCE_TYPE;
  }

  @Override
  public String getDlqSourceType() {
    // TODO: dedicated DLQ source type once retry tooling supports it.
    return Constants.MYSQL_SOURCE_TYPE;
  }

  @Override
  public PipelineResult executeMigration(
      SourceDbToSpannerOptions options,
      SourceConnectionConfig sourceConnectionConfig,
      Pipeline pipeline,
      SpannerConfig spannerConfig) {
    return PipelineController.executeMigrationForDbConfigContainer(
        options,
        pipeline,
        spannerConfig,
        new DbConfigContainerDefaultImpl(GcsIoWrapperFactory.fromConfig(sourceConnectionConfig)));
  }
}
