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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ChangeEventUtils;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertor;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerRecordReaderDoFn extends DoFn<FailsafeElement<String, String>, Struct>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerRecordReaderDoFn.class);

  private final PCollectionView<Ddl> ddlView;

  private final SpannerConfig spannerConfig;

  private transient SpannerAccessor spannerAccessor;

  private transient ObjectMapper mapper;

  SpannerRecordReaderDoFn(SpannerConfig spannerConfig, PCollectionView<Ddl> ddlView) {
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
  }

  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  @Teardown()
  public void teardown() {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    Ddl ddl = c.sideInput(ddlView);

    try {
      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      ChangeEventConvertor.convertChangeEventColumnKeysToLowerCase(changeEvent);
      ChangeEventConvertor.verifySpannerSchema(ddl, changeEvent);
      com.google.cloud.spanner.Key primaryKey =
          ChangeEventSpannerConvertor.changeEventToPrimaryKey(
              changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText(),
              ddl,
              changeEvent,
              /* convertNameToLowerCase= */ true);

      List<String> changeEventKeys = ChangeEventUtils.getEventColumnKeys(changeEvent);
      Struct spannerRecord;
      try (ReadContext readContext = spannerAccessor.getDatabaseClient().singleUse()) {
        spannerRecord =
            readContext.readRow(
                changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText(),
                primaryKey,
                changeEventKeys);
      }
      if (spannerRecord != null) {
        c.output(spannerRecord);
      }
    } catch (Exception e) {
      LOG.error("Unhandled Exception in spanner record reader", e);
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
    }
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }
}
