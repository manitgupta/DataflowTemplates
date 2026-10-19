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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.GcsSourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.templates.DbConfigContainerDefaultImpl;
import com.google.cloud.teleport.v2.templates.PipelineController;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

/** Tests for {@link GcsSrcToSpSourceConnector}. */
@RunWith(JUnit4.class)
public class GcsSrcToSpSourceConnectorTest {

  private final GcsSrcToSpSourceConnector connector = new GcsSrcToSpSourceConnector();

  @Test
  public void testSourceTypes() {
    assertThat(connector.getSourceType()).isEqualTo(Constants.GCS_SOURCE_TYPE);
    assertThat(connector.getSourceType()).isEqualTo("gcs");
    assertThat(connector.getDlqSourceType()).isEqualTo(Constants.MYSQL_SOURCE_TYPE);
  }

  @Test
  public void testExecuteMigration() {
    SourceDbToSpannerOptions options = mock(SourceDbToSpannerOptions.class);
    Pipeline pipeline = mock(Pipeline.class);
    SpannerConfig spannerConfig = SpannerConfig.create();
    PipelineResult expected = mock(PipelineResult.class);
    try (MockedStatic<PipelineController> controller = mockStatic(PipelineController.class)) {
      controller
          .when(
              () ->
                  PipelineController.executeMigrationForDbConfigContainer(
                      eq(options),
                      eq(pipeline),
                      eq(spannerConfig),
                      any(DbConfigContainerDefaultImpl.class)))
          .thenReturn(expected);

      PipelineResult result =
          connector.executeMigration(
              options, new GcsSourceConfig("gs://my-bucket/exp"), pipeline, spannerConfig);

      assertThat(result).isSameInstanceAs(expected);
      controller.verify(
          () ->
              PipelineController.executeMigrationForDbConfigContainer(
                  eq(options),
                  eq(pipeline),
                  eq(spannerConfig),
                  any(DbConfigContainerDefaultImpl.class)));
    }
  }

  @Test
  public void testExecuteMigrationWrongConfigThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            connector.executeMigration(
                mock(SourceDbToSpannerOptions.class),
                mock(SourceConnectionConfig.class),
                mock(Pipeline.class),
                SpannerConfig.create()));
  }
}
