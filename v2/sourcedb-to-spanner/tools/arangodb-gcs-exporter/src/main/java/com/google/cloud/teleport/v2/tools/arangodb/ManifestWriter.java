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
 * Writes {@code manifest.json}. The manifest is the commit marker of an export: it is written last
 * and never overwritten.
 */
public final class ManifestWriter {

  public static final String MANIFEST_FILE = "manifest.json";

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private ManifestWriter() {}

  /**
   * Writes the manifest to {@code <exportRoot>/manifest.json}.
   *
   * @throws UncheckedIOException if the manifest already exists or cannot be written
   */
  public static void write(ExportManifest m, String exportRoot, Configuration hadoopConf) {
    HadoopFiles.writeString(manifestUri(exportRoot), toJson(m) + "\n", hadoopConf, false);
  }

  /** Whether {@code <exportRoot>/manifest.json} exists. */
  public static boolean exists(String exportRoot, Configuration hadoopConf) {
    return HadoopFiles.exists(manifestUri(exportRoot), hadoopConf);
  }

  static String manifestUri(String exportRoot) {
    return exportRoot + "/" + MANIFEST_FILE;
  }

  /** Pretty-printed manifest JSON. */
  static String toJson(ExportManifest m) {
    try {
      return MAPPER.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialise manifest", e);
    }
  }

  /** Parses a manifest (used by tests and tooling). */
  static ExportManifest fromJson(String json) {
    try {
      return MAPPER.readValue(json, ExportManifest.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Cannot parse manifest", e);
    }
  }
}
