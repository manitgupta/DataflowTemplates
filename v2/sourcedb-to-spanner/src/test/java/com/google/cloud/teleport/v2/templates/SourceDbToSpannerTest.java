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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.common.CommonTemplateJvmInitializer;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.AstraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.CassandraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.GcsSourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SourceDbToSpannerTest {

  @Test
  public void testGetSourceDbToSpannerOptions() {
    try (MockedStatic<PipelineOptionsFactory> mockedPipelineOptionsFactory =
            mockStatic(PipelineOptionsFactory.class);
        MockedConstruction<CommonTemplateJvmInitializer> mockedCommonTemplateJvmInitializer =
            mockConstruction(CommonTemplateJvmInitializer.class)) {

      SourceDbToSpannerOptions mockOptions = mock(SourceDbToSpannerOptions.class);
      PipelineOptionsFactory.Builder mockBuilder = mock(PipelineOptionsFactory.Builder.class);

      when(mockBuilder.withValidation()).thenReturn(mockBuilder);
      when(mockBuilder.as(SourceDbToSpannerOptions.class)).thenReturn(mockOptions);
      mockedPipelineOptionsFactory
          .when(() -> PipelineOptionsFactory.fromArgs(any(String[].class)))
          .thenReturn(mockBuilder);

      SourceDbToSpannerOptions options =
          SourceDbToSpanner.getSourceDbToSpannerOptions(new String[] {});

      assertThat(options).isNotNull();
      verify(mockedCommonTemplateJvmInitializer.constructed().get(0), times(1))
          .beforeProcessing(options);
    }
  }

  @Test
  public void testCreateSpannerConfig() {

    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getProjectId()).thenReturn("testProject");
    when(mockOptions.getSpannerHost()).thenReturn("testHost");
    when(mockOptions.getInstanceId()).thenReturn("testInstance");
    when(mockOptions.getDatabaseId()).thenReturn("testDatabaseId");
    when(mockOptions.getMaxCommitDelay()).thenReturn(-1L).thenReturn(42L);

    // For first set of mocks.
    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(mockOptions);
    assertEquals(config.getProjectId().get(), "testProject");
    assertEquals(config.getInstanceId().get(), "testInstance");
    assertEquals(config.getDatabaseId().get(), "testDatabaseId");
    assertThat(config.getMaxCommitDelay()).isNull();

    // For second set of mocks.
    config = SourceDbToSpanner.createSpannerConfig(mockOptions);
    assertEquals(config.getMaxCommitDelay().get(), Duration.millis(42L));
  }

  @Test
  public void testRun_ValidationFailures() {
    SourceDbToSpannerOptions mockOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    mockOptions.setProjectId("testProject");
    mockOptions.setInstanceId("testInstance");
    mockOptions.setDatabaseId("testDatabaseId");
    mockOptions
        .as(org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.class)
        .setWorkerMachineType("n2-standard-4");

    try (MockedStatic<PipelineController> mockedPipelineController =
        mockStatic(PipelineController.class)) {

      // Test MYSQL dialect with wrong config type (AstraConnectionConfig)
      mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT);
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(AstraConnectionConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(mockOptions));

      // Test POSTGRESQL dialect with wrong config type (CassandraConnectionConfig)
      mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.PG_SOURCE_DIALECT);
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(CassandraConnectionConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(mockOptions));

      // Test CASSANDRA dialect with wrong config type (JdbcShardConfig)
      mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT);
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(JdbcShardConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(mockOptions));

      // Test ASTRADB dialect with wrong config type (JdbcShardConfig)
      mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT);
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(JdbcShardConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(mockOptions));

      // Test ORACLE dialect with wrong config type (CassandraConnectionConfig)
      mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.ORACLE_SOURCE_DIALECT);
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(CassandraConnectionConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(mockOptions));
    }
  }

  @Test
  public void testValidateOptions() {
    SourceDbToSpannerOptions mockOptions =
        PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.PG_SOURCE_DIALECT);

    AstraConnectionConfig astraConfig = new AstraConnectionConfig();
    IllegalArgumentException exception1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceDbToSpanner.validateOptions(mockOptions, astraConfig));
    assertEquals("Postgresql dialect should have JDBC source config.", exception1.getMessage());

    JdbcShardConfig jdbcConfig = new JdbcShardConfig();
    Shard shard1 = new Shard();
    shard1.setNamespace("public");
    Shard shard2 = new Shard();
    shard2.setNamespace("custom");
    jdbcConfig.setShardConfigs(Arrays.asList(shard1, shard2));

    IllegalArgumentException exception2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceDbToSpanner.validateOptions(mockOptions, jdbcConfig));
    assertEquals(
        "Non-public namespaces are currently unsupported for PostgreSQL migrations.",
        exception2.getMessage());

    jdbcConfig.setShardConfigs(Arrays.asList(shard1));
    SourceDbToSpanner.validateOptions(mockOptions, jdbcConfig);

    mockOptions.setSourceDbDialect(SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT);
    jdbcConfig.setShardConfigs(Arrays.asList(shard1, shard2));
    SourceDbToSpanner.validateOptions(mockOptions, jdbcConfig);
  }

  @Test
  public void testCreateSpannerConfig_omniUnset_noExperimentalHost() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setProjectId("testProject");
    options.setInstanceId("testInstance");
    options.setDatabaseId("testDatabaseId");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(options);

    assertEquals("testProject", config.getProjectId().get());
    assertEquals("testInstance", config.getInstanceId().get());
    assertThat(config.getExperimentalHost()).isNull();
    assertThat(config.getPlainText()).isNull();
    assertEquals("testProject", options.getProjectId());
  }

  @Test
  public void testCreateSpannerConfig_omniBlank_usesCloudSpanner() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setProjectId("testProject");
    options.setInstanceId("testInstance");
    options.setDatabaseId("testDatabaseId");
    options.setSpannerOmniEndpoint("   ");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(options);

    assertEquals("testProject", config.getProjectId().get());
    assertThat(config.getExperimentalHost()).isNull();
  }

  @Test
  public void testCreateSpannerConfig_omniPlainText() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setProjectId("testProject");
    options.setInstanceId("testInstance");
    options.setDatabaseId("shop");
    options.setSpannerPriority(RpcPriority.MEDIUM);
    options.setSpannerOmniEndpoint("http://localhost:15000");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(options);

    assertEquals("default", config.getProjectId().get());
    assertEquals("default", config.getInstanceId().get());
    assertEquals("shop", config.getDatabaseId().get());
    assertEquals("http://localhost:15000", config.getExperimentalHost().get());
    assertThat(config.getPlainText().get()).isTrue();
    assertEquals(RpcPriority.MEDIUM, config.getRpcPriority().get());
    assertThat(config.getMaxCommitDelay()).isNull();
    // Options are rewritten so downstream code sees the Omni ids.
    assertEquals("default", options.getProjectId());
    assertEquals("default", options.getInstanceId());
  }

  @Test
  public void testCreateSpannerConfig_omniNoPlainText() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setDatabaseId("shop");
    options.setSpannerOmniEndpoint("https://omni.example.com:15000");
    options.setSpannerOmniPlainText(false);

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(options);

    assertEquals("https://omni.example.com:15000", config.getExperimentalHost().get());
    assertThat(config.getPlainText() == null || !config.getPlainText().get()).isTrue();
  }

  @Test
  public void testCreateSpannerConfig_omniMaxCommitDelay() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setDatabaseId("shop");
    options.setSpannerOmniEndpoint("http://localhost:15000");
    options.setMaxCommitDelay(42L);

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(options);

    assertEquals(Duration.millis(42L), config.getMaxCommitDelay().get());
  }

  @Test
  public void testValidateOptions_gcs() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.GCS_SOURCE_DIALECT);

    // Valid config.
    SourceDbToSpanner.validateOptions(
        options, GcsSourceConfig.fromUri("gs://my-bucket/export/manifest.json"));

    // Wrong config type.
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceDbToSpanner.validateOptions(options, new JdbcShardConfig()));
    assertEquals("GCS dialect should have GcsSourceConfig source config.", exception.getMessage());

    // gcsOutputDirectory only warns.
    options.setGcsOutputDirectory("gs://my-bucket/avro");
    SourceDbToSpanner.validateOptions(
        options, GcsSourceConfig.fromUri("gs://my-bucket/export/manifest.json"));
  }

  @Test
  public void testRun_gcsValidationFailure() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setProjectId("testProject");
    options.setInstanceId("testInstance");
    options.setDatabaseId("testDatabaseId");
    options.setSourceDbDialect(SourceDbToSpannerOptions.GCS_SOURCE_DIALECT);
    options
        .as(org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.class)
        .setWorkerMachineType("n2-standard-4");

    try (MockedStatic<PipelineController> mockedPipelineController =
        mockStatic(PipelineController.class)) {
      mockedPipelineController
          .when(() -> PipelineController.getSourceConnectionConfig(any(), any()))
          .thenReturn(mock(JdbcShardConfig.class));
      assertThrows(IllegalArgumentException.class, () -> SourceDbToSpanner.run(options));
    }
  }
}
