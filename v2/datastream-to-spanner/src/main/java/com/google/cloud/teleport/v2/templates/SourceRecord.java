package com.google.cloud.teleport.v2.templates;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** A generic representation of a row read from the database. */
@DefaultCoder(AvroCoder.class)
public class SourceRecord implements Serializable {
  public final Map<String, Object> rowData;

  // No-arg constructor for coder
  public SourceRecord() {
    this.rowData = null;
  }

  public SourceRecord(Map<String, Object> rowData) {
    this.rowData = rowData;
  }

  @Override
  public String toString() {
    return "SourceRecord{" + "rowData=" + rowData + '}';
  }
}
