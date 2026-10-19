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

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
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
  private String root;

  @Before
  public void setUp() throws Exception {
    schema = GcsTestFixtures.documentTable("user_profiles");
    root = tmp.getRoot().getAbsolutePath();
    GcsTestFixtures.writeBulkFile(
        tmp.getRoot(),
        "shard1",
        0,
        2,
        List.of(
            GcsTestFixtures.document(schema, "u1", "{}"),
            GcsTestFixtures.document(schema, "u2", "[]")));
    GcsTestFixtures.writeBulkFile(
        tmp.getRoot(), null, 1, 2, List.of(GcsTestFixtures.document(schema, "u3", "{\"a\":1}")));
    // Another table under the same root, not read.
    GcsTestFixtures.writeBulkFile(
        tmp.getRoot(),
        null,
        0,
        1,
        List.of(GcsTestFixtures.document(GcsTestFixtures.documentTable("other"), "o1", "{}")));
  }

  private GcsAvroTableReader reader(Wait.OnSignal<?> signal) {
    return new GcsAvroTableReader(root, "user_profiles", GcsTestFixtures.REF, schema, 99L, signal);
  }

  private static PCollection<String> keys(PCollection<SourceRow> rows) {
    return rows.apply(
        "Keys",
        MapElements.into(TypeDescriptors.strings())
            .via(
                (SourceRow r) ->
                    r.getPayload().get("key") + "@" + r.shardId() + "@" + r.getReadTimeMicros()));
  }

  @Test
  public void testReadsAllFilesOfTheTable() {
    PCollection<SourceRow> rows = pipeline.apply(reader(null));
    PAssert.that(keys(rows)).containsInAnyOrder("u1@shard1@99", "u2@shard1@99", "u3@null@99");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    long read = 0;
    for (MetricResult<Long> counter :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            GcsAvroRecordToSourceRowFn.class,
                            GcsAvroRecordToSourceRowFn.ROWS_READ_COUNTER_PREFIX + "user_profiles"))
                    .build())
            .getCounters()) {
      read += counter.getAttempted();
    }
    assertThat(read).isEqualTo(3L);
  }

  @Test
  public void testWaitOnSignal() {
    Wait.OnSignal<?> signal = Wait.on(pipeline.apply("Signal", Create.of(1)));
    PCollection<SourceRow> rows = pipeline.apply(reader(signal));
    PAssert.that(keys(rows)).containsInAnyOrder("u1@shard1@99", "u2@shard1@99", "u3@null@99");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingTableReadsNothing() {
    PCollection<SourceRow> rows =
        pipeline.apply(
            new GcsAvroTableReader(root, "gone", GcsTestFixtures.REF, schema, 99L, null));
    PAssert.that(rows).empty();
    pipeline.run().waitUntilFinish();
  }
}
