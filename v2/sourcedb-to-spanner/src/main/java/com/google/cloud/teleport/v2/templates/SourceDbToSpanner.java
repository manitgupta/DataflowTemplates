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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.CommonTemplateJvmInitializer;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.ISrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.SourceConnectorFactory;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.GcsSourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.DataflowWorkerMachineTypeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing Spanner
 * database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Sourcedb_to_Spanner_Flex",
    category = TemplateCategory.BATCH,
    displayName = "Sourcedb to Spanner",
    description = {
      "The SourceDB to Spanner template is a batch pipeline that copies data from a relational"
          + " database into an existing Spanner database. This pipeline uses JDBC to connect to"
          + " the relational database. You can use this template to copy data from any relational"
          + " database with available JDBC drivers into Spanner. This currently only supports a limited set of types of MySQL",
    },
    optionsClass = SourceDbToSpannerOptions.class,
    flexContainerName = "source-db-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC drivers for the relational database must be available.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema.",
      "The relational database must be accessible from the subnet where Dataflow runs."
    })
public class SourceDbToSpanner {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);

  /** Project and instance id used by Spanner Omni, which has no project or instance concept. */
  @VisibleForTesting static final String SPANNER_OMNI_DEFAULT_ID = "default";

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link SourceDbToSpanner#run} method to start the
   * pipeline and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    SourceDbToSpannerOptions options = getSourceDbToSpannerOptions(args);
    run(options);
  }

  @VisibleForTesting
  protected static SourceDbToSpannerOptions getSourceDbToSpannerOptions(String[] args) {
    // Parse the user options passed from the command-line
    SourceDbToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SourceDbToSpannerOptions.class);
    // Stage SSL certificates to extraFiles if required as per the pipeline options.
    // Ref https://cloud.google.com/dataflow/docs/guides/templates/ssl-certificates
    new CommonTemplateJvmInitializer().beforeProcessing(options);
    return options;
  }

  /**
   * Validates the provided pipeline options. TODO: move this to source connector.
   *
   * @param options The execution parameters to the pipeline.
   * @param sourceConnectionConfig Parsed source connection config.
   * @throws IllegalArgumentException if the provided options are invalid for the pipeline.
   */
  @VisibleForTesting
  static void validateOptions(
      SourceDbToSpannerOptions options, SourceConnectionConfig sourceConnectionConfig) {
    if (SourceDbToSpannerOptions.PG_SOURCE_DIALECT.equals(options.getSourceDbDialect())) {
      Preconditions.checkArgument(
          (sourceConnectionConfig instanceof JdbcShardConfig),
          "Postgresql dialect should have JDBC source config.");
      for (Shard shard : ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs()) {
        if (StringUtils.isNotBlank(shard.getNamespace())
            && !shard.getNamespace().equals("public")) {
          throw new IllegalArgumentException(
              "Non-public namespaces are currently unsupported for PostgreSQL migrations.");
        }
      }
    }
    if (SourceDbToSpannerOptions.GCS_SOURCE_DIALECT.equals(options.getSourceDbDialect())) {
      Preconditions.checkArgument(
          sourceConnectionConfig instanceof GcsSourceConfig,
          "GCS dialect should have GcsSourceConfig source config.");
      if (StringUtils.isNotBlank(options.getGcsOutputDirectory())) {
        LOG.warn(
            "gcsOutputDirectory is set for GCS source; source rows will be re-written as Avro.");
      }
    }
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(SourceDbToSpannerOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    String workerMachineType =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getWorkerMachineType();
    Optional<Integer> resourceHintsMinCpus =
        DataflowWorkerMachineTypeUtils.getMinCpuResourceHint(pipeline.getOptions());
    if (!pipeline.getOptions().getRunner().getSimpleName().equals("DirectRunner")) {
      DataflowWorkerMachineTypeUtils.validateMachineSpecs(
          workerMachineType, 4, resourceHintsMinCpus);
    }

    SpannerConfig spannerConfig = createSpannerConfig(options);
    SourceConnectionConfig sourceConnectionConfig =
        PipelineController.getSourceConnectionConfig(
            options.getSourceDbDialect(), options.getSourceConfigURL());

    validateOptions(options, sourceConnectionConfig);

    // Decide type and source of migration
    ISrcToSpSourceConnector connector = SourceConnectorFactory.getSourceConnectorByDialect(options);
    return connector.executeMigration(options, sourceConnectionConfig, pipeline, spannerConfig);
  }

  /**
   * Creates the {@link SpannerConfig} for the target database.
   *
   * <p>When {@code spannerOmniEndpoint} is set, the config targets a self-managed Spanner Omni
   * deployment through the experimental host, with {@code default} as project and instance id (the
   * pipeline options are updated accordingly so that downstream code sees consistent values).
   *
   * @param options The execution parameters to the pipeline.
   * @return The Spanner config.
   */
  @VisibleForTesting
  static SpannerConfig createSpannerConfig(SourceDbToSpannerOptions options) {
    String omniEndpoint = options.getSpannerOmniEndpoint();
    if (StringUtils.isNotBlank(omniEndpoint)) {
      return createSpannerOmniConfig(options, omniEndpoint.trim());
    }
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));
    if (options.getMaxCommitDelay() >= 0) {
      spannerConfig = spannerConfig.withMaxCommitDelay(options.getMaxCommitDelay());
    }
    return spannerConfig;
  }

  private static SpannerConfig createSpannerOmniConfig(
      SourceDbToSpannerOptions options, String omniEndpoint) {
    LOG.info("Writing to Spanner Omni at {}", omniEndpoint);
    // Spanner Omni has no project / instance; the client uses "default" for both.
    options.setProjectId(SPANNER_OMNI_DEFAULT_ID);
    options.setInstanceId(SPANNER_OMNI_DEFAULT_ID);
    SpannerConfig omniConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(SPANNER_OMNI_DEFAULT_ID))
            .withInstanceId(ValueProvider.StaticValueProvider.of(SPANNER_OMNI_DEFAULT_ID))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
            .withExperimentalHost(ValueProvider.StaticValueProvider.of(omniEndpoint))
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));
    if (Boolean.TRUE.equals(options.getSpannerOmniPlainText())) {
      omniConfig = omniConfig.withUsingPlainTextChannel(true);
    }
    if (options.getMaxCommitDelay() >= 0) {
      omniConfig = omniConfig.withMaxCommitDelay(options.getMaxCommitDelay());
    }
    return omniConfig;
  }
}
