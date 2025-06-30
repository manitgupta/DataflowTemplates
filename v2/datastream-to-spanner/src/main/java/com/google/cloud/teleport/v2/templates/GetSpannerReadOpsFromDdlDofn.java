package com.google.cloud.teleport.v2.templates;

import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;

public class GetSpannerReadOpsFromDdlDofn extends DoFn<String, ReadOperation> implements Serializable {

  @ProcessElement
  public void processElement(ProcessContext c) {
    String tableName = c.element();
    c.output(ReadOperation.create().withQuery("SELECT *, '" + tableName + "' AS source_table FROM " + tableName));
  }
}
