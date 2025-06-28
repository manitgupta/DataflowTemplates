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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class SpannerRecordReader
    extends PTransform<PCollection<FailsafeElement<String, String>>, SpannerRecordReader.Result> {

  private final SpannerConfig spannerConfig;

  private final PCollectionView<Ddl> ddlView;

  public SpannerRecordReader(SpannerConfig spannerConfig, PCollectionView<Ddl> ddlView) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
  }

  @Override
  public Result expand(PCollection<FailsafeElement<String, String>> input) {
    PCollectionTuple spannerReadResults =
        input.apply(
            "Read records from Spanner",
            ParDo.of(new SpannerRecordReaderDoFn(spannerConfig, ddlView))
                .withSideInputs(ddlView)
                .withOutputTags(
                    DatastreamToSpannerConstants.SPANNER_RECORDS,
                    TupleTagList.of(
                        Arrays.asList(DatastreamToSpannerConstants.SPANNER_READ_ERRORS))));

    return Result.create(
        spannerReadResults.get(DatastreamToSpannerConstants.SPANNER_RECORDS),
        spannerReadResults.get(DatastreamToSpannerConstants.SPANNER_READ_ERRORS));
  }

  @AutoValue
  public abstract static class Result implements POutput {

    private static SpannerRecordReader.Result create(
        PCollection<Struct> spannerRecords, PCollection<FailsafeElement<String, String>> errors) {
      Preconditions.checkNotNull(spannerRecords);
      Preconditions.checkNotNull(errors);
      return new AutoValue_SpannerRecordReader_Result(spannerRecords, errors);
    }

    public abstract PCollection<Struct> spannerRecords();

    public abstract PCollection<FailsafeElement<String, String>> errors();

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      // required by POutput interface.
    }

    @Override
    public Pipeline getPipeline() {
      return spannerRecords().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          DatastreamToSpannerConstants.SPANNER_RECORDS,
          spannerRecords(),
          DatastreamToSpannerConstants.SPANNER_READ_ERRORS,
          errors());
    }
  }
}
