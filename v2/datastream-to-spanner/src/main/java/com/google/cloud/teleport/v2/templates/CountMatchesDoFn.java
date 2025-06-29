/*
 * Copyright (C) 2025 Google LLC
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

import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMatchesDoFn extends DoFn<KV<String, CoGbkResult>, KV<String, Long>> {

  private static final Logger LOG = LoggerFactory.getLogger(CountMatchesDoFn.class);
  public static final String TOTAL_MATCHES = "totalMatches:";
  public static final String TOTAL_SOURCE_RECORD_COUNT = "totalSourceRecordCount:";
  public static final String TOTAL_TARGET_RECORD_COUNT = "totalTargetRecordCount:";
  public static final String TOTAL_UNMATCHED_TARGET_RECORD_COUNT =
      "totalUnmatchedTargetRecordCount:";
  public static final String TOTAL_UNMATCHED_SOURCE_RECORD_COUNT =
      "totalUnmatchedSourceRecordCount:";

  @ProcessElement
  public void processElement(ProcessContext c, MultiOutputReceiver out) {
    KV<String, CoGbkResult> e = c.element();
    Iterable<String> sourceRecords = e.getValue().getAll(DatastreamToSpannerConstants.SOURCE_TAG);
    Iterable<String> spannerRecords = e.getValue().getAll(DatastreamToSpannerConstants.SPANNER_TAG);

    String sourceRecord = null, spannerRecord = null;
    if (sourceRecords.iterator().hasNext()) {
      sourceRecord = sourceRecords.iterator().next();
    }
    if (spannerRecords.iterator().hasNext()) {
      spannerRecord = spannerRecords.iterator().next();
    }
    if (spannerRecord != null && sourceRecord != null) {
      String tableName = getTableFromRecord(sourceRecord);
      out.get(DatastreamToSpannerConstants.MATCHED_RECORDS_TAG).output(KV.of(TOTAL_MATCHES + tableName, 1L));
      out.get(DatastreamToSpannerConstants.SOURCE_RECORDS_TAG)
          .output(KV.of(TOTAL_SOURCE_RECORD_COUNT + tableName, 1L));
      out.get(DatastreamToSpannerConstants.TARGET_RECORDS_TAG)
          .output(KV.of(TOTAL_TARGET_RECORD_COUNT + tableName, 1L));
    } else if (spannerRecord != null) { //record found in spanner but not in source
      String tableName = getTableFromRecord(spannerRecord);
      out.get(DatastreamToSpannerConstants.UNMATCHED_TARGET_RECORDS_TAG)
          .output(KV.of(TOTAL_UNMATCHED_TARGET_RECORD_COUNT + tableName, 1L));
      out.get(DatastreamToSpannerConstants.TARGET_RECORDS_TAG)
          .output(KV.of(TOTAL_TARGET_RECORD_COUNT + tableName, 1L));
      out.get(DatastreamToSpannerConstants.UNMATCHED_TARGET_RECORD_VALUES_TAG)
          .output(spannerRecord);
    } else if (sourceRecord != null) {
      String tableName = getTableFromRecord(sourceRecord);
      out.get(DatastreamToSpannerConstants.UNMATCHED_SOURCE_RECORDS_TAG)
          .output(KV.of(TOTAL_UNMATCHED_SOURCE_RECORD_COUNT + tableName, 1L));
      out.get(DatastreamToSpannerConstants.SOURCE_RECORDS_TAG)
          .output(KV.of(TOTAL_SOURCE_RECORD_COUNT + tableName, 1L));
      out.get(DatastreamToSpannerConstants.UNMATCHED_SOURCE_RECORD_VALUES_TAG).output(sourceRecord);
    }
  }

  private String getTableFromRecord(String recordString) {
    return recordString.substring(1, recordString.indexOf('#', 1)); //first hash is always at zero index
  }
}
