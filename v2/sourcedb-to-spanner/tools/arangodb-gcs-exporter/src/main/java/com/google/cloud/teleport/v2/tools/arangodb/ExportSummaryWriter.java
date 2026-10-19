/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.tools.arangodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.UncheckedIOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Writes the export summary {@code arangodb-export.json}. It is written last and never overwritten,
 * so its presence marks a complete export. The data itself needs no summary: it is a plain SourceDB
 * to Spanner bulk GCS export that the template reads by directory layout.
 */
public final class ExportSummaryWriter {

  public static final String SUMMARY_FILE = "arangodb-export.json";

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private ExportSummaryWriter() {}

  /**
   * Writes the summary to {@code <exportRoot>/arangodb-export.json}.
   *
   * @throws UncheckedIOException if the summary already exists or cannot be written
   */
  public static void write(ExportSummary m, String exportRoot, Configuration hadoopConf) {
    HadoopFiles.writeString(summaryUri(exportRoot), toJson(m) + "\n", hadoopConf, false);
  }

  /** Whether {@code <exportRoot>/arangodb-export.json} exists. */
  public static boolean exists(String exportRoot, Configuration hadoopConf) {
    return HadoopFiles.exists(summaryUri(exportRoot), hadoopConf);
  }

  static String summaryUri(String exportRoot) {
    return exportRoot + "/" + SUMMARY_FILE;
  }

  /** Pretty-printed summary JSON. */
  static String toJson(ExportSummary m) {
    try {
      return MAPPER.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialise export summary", e);
    }
  }

  /** Parses a summary (used by tests and tooling). */
  static ExportSummary fromJson(String json) {
    try {
      return MAPPER.readValue(json, ExportSummary.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot parse export summary", e);
    }
  }
}
