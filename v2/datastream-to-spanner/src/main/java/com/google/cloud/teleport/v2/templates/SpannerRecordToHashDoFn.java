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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerRecordToHashDoFn extends DoFn<Struct, KV<String, String>>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerRecordToHashDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    Struct spannerStruct = c.element();
    int nCols = spannerStruct.getColumnCount();
    StringBuilder sbConcatCols = new StringBuilder();
    for (int i = 0; i < nCols; i++) {
      Type colType = spannerStruct.getColumnType(i);

      switch (colType.toString()) {
        case "STRING":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getString(i));
          break;
        case "BYTES":
          sbConcatCols.append(
              spannerStruct.isNull(i)
                  ? ""
                  : Base64.encodeBase64String(spannerStruct.getBytes(i).toByteArray()));
          break;
        case "INT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getLong(i));
          break;
        case "FLOAT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getDouble(i));
          break;
        case "NUMERIC":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBigDecimal(i));
          break;
        case "TIMESTAMP":
          // TODO: This uses millisecond precision; consider using microsecond precision
          if (!spannerStruct.isNull(i)) {
            Long rawTimestamp = spannerStruct.getTimestamp(i).toSqlTimestamp().getTime();
            sbConcatCols.append(rawTimestamp);
          }
          break;
        case "DATE":
          if (!spannerStruct.isNull(i)) {
            com.google.cloud.Date date = spannerStruct.getDate(i);
            sbConcatCols.append(
                String.format("%d%d%d", date.getYear(), date.getMonth(), date.getDayOfMonth()));
          }
          break;
        case "BOOL":
        case "BOOLEAN":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBoolean(i));
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type: %s", colType));
      } // switch
    } // for
    String hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(sbConcatCols.toString());
    LOG.info("Spanner Hash = {} Spanner payload: {}", hash, sbConcatCols);
    c.output(KV.of(hash, sbConcatCols.toString()));
  }
}
