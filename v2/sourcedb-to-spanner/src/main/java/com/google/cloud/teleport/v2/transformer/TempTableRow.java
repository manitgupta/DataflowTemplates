package com.google.cloud.teleport.v2.transformer;

import java.io.Serializable;

public class TempTableRow implements Serializable {

  private final java.util.Map<String, Object> internalMap = new java.util.HashMap<>();

  public TempTableRow set(String fieldName, Object value) {
    internalMap.put(fieldName, value);
    return this;
  }

  public java.util.Map<String, Object> getInternalMap() {
    return internalMap;
  }

}
