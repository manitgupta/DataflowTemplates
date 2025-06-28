package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
public class SpannerRecord {
  private final String tableName;
  private final Struct spannerRecord;

  public SpannerRecord(String tableName, Struct spannerRecord) {
    this.tableName = tableName;
    this.spannerRecord = spannerRecord;
  }

  public String getTableName() {
    return tableName;
  }

  public Struct getSpannerRecord() {
    return spannerRecord;
  }
}
