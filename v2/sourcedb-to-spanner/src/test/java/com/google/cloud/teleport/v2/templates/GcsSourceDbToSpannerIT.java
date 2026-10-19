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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link SourceDbToSpanner} with the {@code GCS} dialect: migrates an
 * ArangoDB-style Avro export in the bulk migration format ({@code
 * <root>/<tableName>/<md5>-NNNNN-of-MMMMM.avro}) from Cloud Storage into Spanner.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class GcsSourceDbToSpannerIT extends SourceDbToSpannerITBase {

  private static final String RESOURCE_DIR = "GcsSourceDbToSpannerIT/";
  private static final String SPANNER_DDL_RESOURCE = RESOURCE_DIR + "spanner-schema.sql";
  private static final List<String> EXPORT_FILES =
      List.of(
          "export/user_profiles/b36539a80425c7f2c6178856a7bb935f-00000-of-00001.avro",
          "export/knows/a7f80905c6bc6e9912edef8c18d80a52-00000-of-00001.avro");

  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws IOException {
    for (String file : EXPORT_FILES) {
      gcsClient.uploadArtifact(file, Resources.getResource(RESOURCE_DIR + file).getPath());
    }
    spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void gcsExportToSpanner() throws IOException {
    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("sourceDbDialect", "GCS");
    jobParameters.put("sourceConfigURL", getGcsPath("export", gcsClient));
    jobParameters.put("outputDirectory", getGcsPath("output", gcsClient));

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(testName, null, null, null, spannerResourceManager, jobParameters, null);
    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, java.time.Duration.ofMinutes(30L)));
    assertThatResult(result).isLaunchFinished();

    // Documents.
    assertThat(spannerResourceManager.getRowCount("user_profiles")).isEqualTo(3L);
    List<Struct> users = spannerResourceManager.readTableRecords("user_profiles", "key", "name");
    SpannerAsserts.assertThatStructs(users)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of("key", "u1", "name", "Ann"),
                Map.of("key", "u2", "name", "Bob"),
                Map.of("key", "u3", "name", "Chloe")));

    // Edges, including the generated helper columns.
    assertThat(spannerResourceManager.getRowCount("knows")).isEqualTo(2L);
    List<Struct> edges =
        spannerResourceManager.readTableRecords("knows", "key", "from_id", "from_key", "to_key");
    SpannerAsserts.assertThatStructs(edges)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of(
                    "key", "e1", "from_id", "user-profiles/u1", "from_key", "u1", "to_key", "u2"),
                Map.of(
                    "key", "e2", "from_id", "user-profiles/u2", "from_key", "u2", "to_key", "u3")));

    // No severe errors.
    List<Artifact> severe =
        gcsClient.listArtifacts("output/dlq/severe/", Pattern.compile("^(?!.*/tmp/).*\\.json$"));
    assertThat(severe).isEmpty();
  }
}
