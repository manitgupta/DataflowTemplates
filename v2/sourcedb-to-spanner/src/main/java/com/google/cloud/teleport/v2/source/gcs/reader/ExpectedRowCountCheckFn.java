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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares the number of rows read for a table with the manifest's {@code rowCount}. A mismatch is
 * logged and counted but does not fail the pipeline.
 */
final class ExpectedRowCountCheckFn extends DoFn<Long, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpectedRowCountCheckFn.class);

  /** Prefix of the mismatch counter; the table name is appended. */
  static final String MISMATCH_COUNTER_PREFIX = "gcs_source_rowcount_mismatch_";

  private final String table;
  private final long expected;
  private final Counter mismatches;

  ExpectedRowCountCheckFn(String table, long expected) {
    this.table = table;
    this.expected = expected;
    this.mismatches = Metrics.counter(GcsAvroTableReader.class, MISMATCH_COUNTER_PREFIX + table);
  }

  @ProcessElement
  public void processElement(@Element Long actual) {
    if (actual == null || actual != expected) {
      mismatches.inc();
      LOG.error(
          "Row count mismatch for GCS source table {}: manifest rowCount={}, rows read={}",
          table,
          expected,
          actual);
    } else {
      LOG.info("Row count for GCS source table {} matches the manifest: {}", table, actual);
    }
  }
}
