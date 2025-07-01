package com.google.cloud.teleport.v2.templates;

import java.io.Serializable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
public class SourceField implements Serializable {

  private final String fieldName;

  private final String fieldDataType;

  private final Object fieldValue;

  public SourceField(String fieldName, String fieldDataType, Object fieldValue) {
    this.fieldName = fieldName;
    this.fieldDataType = fieldDataType;
    this.fieldValue = fieldValue;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldDataType() {
    return fieldDataType;
  }

  public Object getFieldValue() {
    return fieldValue;
  }

  @Override
  public String toString() {
    return "SourceField{" +
        "fieldName='" + fieldName + '\'' +
        ", fieldDataType='" + fieldDataType + '\'' +
        ", fieldValue=" + fieldValue +
        '}';
  }
}

