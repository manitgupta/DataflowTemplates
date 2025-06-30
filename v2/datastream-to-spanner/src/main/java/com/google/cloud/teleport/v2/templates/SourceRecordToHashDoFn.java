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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceRecordToHashDoFn
    extends DoFn<SourceRecord, KV<String, String>> implements Serializable {

  private transient ObjectMapper mapper;
  private static final Logger LOG = LoggerFactory.getLogger(SourceRecordToHashDoFn.class);

  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    SourceRecord sourceRecord = c.element();
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("#%s#",sourceRecord.rowData.get("source_table")));
      for (String key: sourceRecord.rowData.keySet()) {
        if (key.equals("source_table")) {
          continue;
        }
        if (sourceRecord.rowData.get(key) != null) {
          sb.append(key);
          sb.append(sourceRecord.rowData.get(key));
        }
      }
      String hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(sb.toString());
      // LOG.info("Source Hash = {}, Source payload: {}", hash, sb.toString());
      c.output(KV.of(hash, sb.toString()));
    } catch (Exception e) {
      LOG.error("Unhandled Exception in datastream to hash dofn", e);
    }
  }
}
