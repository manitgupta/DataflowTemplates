package com.google.cloud.teleport.v2.templates;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
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

public class DataStreamRecordsToSpannerMutations
    extends PTransform<PCollection<FailsafeElement<String, String>>, DataStreamRecordsToSpannerMutations.Result> {

  private final TransformationContext transformationContext;
  private final PCollectionView<Ddl> ddlView;
  private final Schema schema;
  private final String sourceType;

  public static final TupleTag<FailsafeElement<String, String>> PERMANENT_ERROR_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* The Tag for retryable Failed mutations */
  public static final TupleTag<FailsafeElement<String, String>> RETRYABLE_ERROR_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* The Tag for Successful mutations */
  public static final TupleTag<Mutation> SUCCESSFUL_EVENT_TAG = new TupleTag<Mutation>() {};



  public DataStreamRecordsToSpannerMutations(TransformationContext transformationContext, PCollectionView<Ddl> ddlView, Schema schema, String sourceType) {
    this.transformationContext = transformationContext;
    this.ddlView = ddlView;
    this.schema = schema;
    this.sourceType = sourceType;
  }

  @Override
  public DataStreamRecordsToSpannerMutations.Result expand(PCollection<FailsafeElement<String, String>> datastreamJsonRecords) {
    PCollectionTuple spannerMutations = datastreamJsonRecords.apply("Convert Datastream record to Spanner mutation",
        ParDo.of(new DataStreamRecordsToSpannerMutationsDoFn(transformationContext, ddlView, schema, sourceType))
            .withSideInputs(ddlView)
            .withOutputTags(
                SUCCESSFUL_EVENT_TAG,
                TupleTagList.of(Arrays.asList(PERMANENT_ERROR_TAG, RETRYABLE_ERROR_TAG))));
    return Result.create(spannerMutations.get(SUCCESSFUL_EVENT_TAG),
        spannerMutations.get(PERMANENT_ERROR_TAG),
        spannerMutations.get(RETRYABLE_ERROR_TAG));
  }

  @AutoValue
  public abstract static class Result implements POutput {

    private static Result create(
        PCollection<Mutation> successfulSpannerWrites,
        PCollection<FailsafeElement<String, String>> permanentErrors,
        PCollection<FailsafeElement<String, String>> retryableErrors) {
      Preconditions.checkNotNull(successfulSpannerWrites);
      Preconditions.checkNotNull(permanentErrors);
      Preconditions.checkNotNull(retryableErrors);
      return new AutoValue_DataStreamRecordsToSpannerMutations_Result(
          successfulSpannerWrites, permanentErrors, retryableErrors);
    }

    public abstract PCollection<Mutation> successfulSpannerWrites();

    public abstract PCollection<FailsafeElement<String, String>> permanentErrors();

    public abstract PCollection<FailsafeElement<String, String>> retryableErrors();

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      // required by POutput interface.
    }

    @Override
    public Pipeline getPipeline() {
      return successfulSpannerWrites().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          SUCCESSFUL_EVENT_TAG,
          successfulSpannerWrites(),
          PERMANENT_ERROR_TAG,
          permanentErrors(),
          RETRYABLE_ERROR_TAG,
          retryableErrors());
    }
  }
}
