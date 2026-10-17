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
package com.google.cloud.teleport.v2.source.gcs.reader;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import java.io.File;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsAvroTableReader}. */
@RunWith(JUnit4.class)
public class GcsAvroTableReaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private SourceTableSchema schema;
  private String file1;
  private String file2;

  @Before
  public void setUp() throws Exception {
    schema =
        GcsAvroRecordToSourceRowFnTest.tableSchema(
            "user_profiles", GcsAvroRecordToSourceRowFnTest.DOC_COLUMNS);
    file1 =
        GcsTestFixtures.writeAvro(
                new File(tmp.getRoot(), "tables/user_profiles/part-0.avro"),
                GcsTestFixtures.DOCUMENT_SCHEMA,
                List.of(GcsTestFixtures.document("u1", "{}"), GcsTestFixtures.document("u2", "[]")))
            .getAbsolutePath();
    file2 =
        GcsTestFixtures.writeAvro(
                new File(tmp.getRoot(), "tables/user_profiles/part-1.avro"),
                GcsTestFixtures.DOCUMENT_SCHEMA,
                List.of(GcsTestFixtures.document("u3", "{\"a\":1}")))
            .getAbsolutePath();
  }

  private GcsAvroTableReader reader(
      List<String> files, Long expectedRowCount, Wait.OnSignal<?> signal) {
    return new GcsAvroTableReader(
        files,
        GcsAvroRecordToSourceRowFnTest.REF,
        schema,
        GcsAvroRecordToSourceRowFnTest.DOC_COLUMNS,
        99L,
        expectedRowCount,
        signal);
  }

  private static PCollection<String> keys(PCollection<SourceRow> rows) {
    return rows.apply(
        "Keys",
        MapElements.into(TypeDescriptors.strings())
            .via((SourceRow r) -> r.getPayload().get("key").toString()));
  }

  private static long mismatchCounter(PipelineResult result, String table) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            GcsAvroTableReader.class,
                            ExpectedRowCountCheckFn.MISMATCH_COUNTER_PREFIX + table))
                    .build());
    long total = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }

  @Test
  public void testReadsAllFiles() {
    PCollection<SourceRow> rows = pipeline.apply(reader(List.of(file1, file2), 3L, null));
    PAssert.that(keys(rows)).containsInAnyOrder("u1", "u2", "u3");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertThat(mismatchCounter(result, "user_profiles")).isEqualTo(0L);
  }

  @Test
  public void testWaitOnSignal() {
    Wait.OnSignal<?> signal = Wait.on(pipeline.apply("Signal", Create.of(1)));
    PCollection<SourceRow> rows = pipeline.apply(reader(List.of(file1, file2), null, signal));
    PAssert.that(keys(rows)).containsInAnyOrder("u1", "u2", "u3");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testGlobPattern() {
    String glob = new File(tmp.getRoot(), "tables/user_profiles/*.avro").getAbsolutePath();
    PCollection<SourceRow> rows = pipeline.apply(reader(List.of(glob), null, null));
    PAssert.that(keys(rows)).containsInAnyOrder("u1", "u2", "u3");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingListedFileFails() {
    String missing = new File(tmp.getRoot(), "tables/user_profiles/gone.avro").getAbsolutePath();
    pipeline.apply(reader(List.of(file1, missing), null, null));
    assertThrows(Pipeline.PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void testRowCountMismatchIncrementsCounter() {
    PCollection<SourceRow> rows = pipeline.apply(reader(List.of(file1, file2), 5L, null));
    PAssert.that(keys(rows)).containsInAnyOrder("u1", "u2", "u3");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertThat(mismatchCounter(result, "user_profiles")).isEqualTo(1L);
  }

  @Test
  public void testEmptyFileList() {
    PCollection<SourceRow> rows = pipeline.apply(reader(List.of(), 0L, null));
    PAssert.that(rows).empty();
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertThat(mismatchCounter(result, "user_profiles")).isEqualTo(0L);
  }
}
