package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

public class DataStreamRecordsToSpannerMutations
    extends PTransform<PCollection<FailsafeElement<String, String>>, SpannerWriteResult> {

  private final TransformationContext transformationContext;
  private final PCollectionView<Ddl> ddlView;
  private final Schema schema;
  private final String sourceType;
  private final SpannerConfig spannerConfig;

  public DataStreamRecordsToSpannerMutations(TransformationContext transformationContext, PCollectionView<Ddl> ddlView, Schema schema, String sourceType, SpannerConfig spannerConfig) {
    this.transformationContext = transformationContext;
    this.ddlView = ddlView;
    this.schema = schema;
    this.sourceType = sourceType;
    this.spannerConfig = spannerConfig;
  }

  @Override
  public SpannerWriteResult expand(PCollection<FailsafeElement<String, String>> datastreamJsonRecords) {
    PCollection<Mutation> spannerMutations = datastreamJsonRecords.apply("Convert Datastream record to Spanner mutation",
        ParDo.of(new DataStreamRecordsToSpannerMutationsDoFn(transformationContext, ddlView, schema, sourceType))
            .withSideInputs(ddlView)).setCoder(SerializableCoder.of(Mutation.class));

    return spannerMutations
        .setCoder(SerializableCoder.of(Mutation.class))
        .apply("Write mutations",
        SpannerIO.write()
            .withSpannerConfig(spannerConfig)
            .withCommitDeadline(Duration.standardMinutes(1))
            .withMaxCumulativeBackoff(Duration.standardHours(2))
            .withMaxNumMutations(10000)
            .withGroupingFactor(100));
  }
}
