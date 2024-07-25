package com.google.cloud.teleport.v2.datastream.sources;

import com.google.cloud.teleport.v2.datastream.sources.DataStreamIO.ExtractGcsFile;
import com.google.common.base.Strings;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class DataStreamFileIO extends PTransform<PBegin, PCollection<Metadata>> {

  private final String gcsNotificationSubscription;

  public DataStreamFileIO(String gcsNotificationSubscription) {
    this.gcsNotificationSubscription = gcsNotificationSubscription;
  }

  @Override
  public PCollection<Metadata> expand(PBegin input) {
    PCollection<Metadata> datastreamFiles;
    if (!Strings.isNullOrEmpty(gcsNotificationSubscription)) {
      datastreamFiles = expandGcsPubSubPipeline(input);
    } else {
      throw new IllegalArgumentException(
          "DataStreamIO requires either a GCS stream directory or Pub/Sub Subscription");
    }

    return datastreamFiles;
  }

  public PCollection<Metadata> expandGcsPubSubPipeline(PBegin input) {
    return input
        .apply(
            "ReadGcsPubSubSubscription",
            PubsubIO.readMessagesWithAttributes().fromSubscription(gcsNotificationSubscription))
        .apply("ExtractGcsFilePath", ParDo.of(new ExtractGcsFile()));
  }
}