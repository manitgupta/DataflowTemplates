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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GCSSpannerDVTest {

  private GCSSpannerDVOptions options;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(GCSSpannerDVOptions.class);
    // Set required options to bypass early validation (if any)
    options.setGcsInputDirectory("gs://dummy/input");
    options.setProjectId("test-project");
    options.setInstanceId("test-instance");
    options.setDatabaseId("test-database");
    options.setBigQueryDataset("test_dataset");
  }

  @Test
  public void testCreateSpannerConfig() {
    assertNotNull(GCSSpannerDV.createSpannerConfig(options));
  }

  @Test
  public void testCreateSpannerConfigForSpannerOmni() {
    options.setSpannerOmniEndpoint("http://10.0.0.5:15000");
    org.apache.beam.sdk.io.gcp.spanner.SpannerConfig config =
        GCSSpannerDV.createSpannerConfig(options);
    org.junit.Assert.assertEquals("http://10.0.0.5:15000", config.getExperimentalHost().get());
    org.junit.Assert.assertEquals("default", config.getProjectId().get());
    org.junit.Assert.assertEquals("default", config.getInstanceId().get());
    org.junit.Assert.assertEquals("test-database", config.getDatabaseId().get());
    org.junit.Assert.assertTrue(config.getPlainText().get());
    org.junit.Assert.assertEquals("default", options.getInstanceId());
  }

  @Test
  public void testCreateSpannerConfigForSpannerOmniReadTimeout() {
    options.setSpannerOmniEndpoint("http://10.0.0.5:15000");
    org.junit.Assert.assertNull(
        GCSSpannerDV.createSpannerConfig(options).getExecuteStreamingSqlRetrySettings());
    options.setSpannerOmniReadTimeoutMinutes(15);
    org.apache.beam.sdk.io.gcp.spanner.SpannerConfig config =
        GCSSpannerDV.createSpannerConfig(options);
    org.junit.Assert.assertEquals(
        java.time.Duration.ofMinutes(15),
        config.getExecuteStreamingSqlRetrySettings().getTotalTimeoutDuration());
  }

  @Test
  public void testCreateSpannerConfigForSpannerOmniTls() {
    options.setSpannerOmniEndpoint("https://omni:15000");
    options.setSpannerOmniPlainText(false);
    org.apache.beam.sdk.io.gcp.spanner.SpannerConfig config =
        GCSSpannerDV.createSpannerConfig(options);
    org.junit.Assert.assertEquals("https://omni:15000", config.getExperimentalHost().get());
    org.junit.Assert.assertTrue(config.getPlainText() == null || !config.getPlainText().get());
  }

  @Test
  public void testRunThrowsExceptionWhenBothTableConfigsProvided() {
    options.setTables("table1,table2");
    options.setTableConfigurationFilePath("gs://dummy/tables.json");

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> GCSSpannerDV.run(options));

    assertTrue(
        thrown.getMessage().contains("Please configure only one of these parameters at a time"));
  }

  @Test
  public void testRunThrowsExceptionWhenTableConfigurationFileFailsToRead() {
    options.setTableConfigurationFilePath("non_existent_file.json");
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> GCSSpannerDV.run(options));
    assertTrue(thrown.getMessage().contains("Failed to read JSON tableConfigurationFilePath"));
  }
}
