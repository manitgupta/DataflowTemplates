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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * Source config for the GCS (manifest + Avro files) source; holds only the manifest URI.
 *
 * <p>The manifest URI may point to any Beam FileSystem (gs://, s3://, local paths, ...). No I/O is
 * performed when the config is created; the manifest is read by the source connector.
 */
public final class GcsSourceConfig implements SourceConnectionConfig, Serializable {

  /** File name of the export manifest, appended to directory URIs ending in {@code /}. */
  public static final String MANIFEST_FILE_NAME = "manifest.json";

  private final String manifestPath;

  /**
   * Creates a config pointing to the given manifest.
   *
   * @param manifestPath URI of the export manifest.
   * @throws IllegalArgumentException if {@code manifestPath} is null or blank.
   */
  public GcsSourceConfig(String manifestPath) {
    if (manifestPath == null || manifestPath.trim().isEmpty()) {
      throw new IllegalArgumentException("GCS source manifest path must not be blank.");
    }
    this.manifestPath = manifestPath;
  }

  /**
   * Creates a config from a manifest or export-directory URI. A URI ending in {@code /} is treated
   * as the export directory and {@link #MANIFEST_FILE_NAME} is appended to it.
   *
   * @param uri URI of the manifest, or of the export directory ending in {@code /}.
   * @return the config.
   * @throws IllegalArgumentException if {@code uri} is null or blank.
   */
  public static GcsSourceConfig fromUri(String uri) {
    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "sourceConfigURL must point to the export manifest for the GCS source.");
    }
    String trimmed = uri.trim();
    if (trimmed.endsWith("/")) {
      trimmed = trimmed + MANIFEST_FILE_NAME;
    }
    return new GcsSourceConfig(trimmed);
  }

  /** Returns the URI of the export manifest. */
  public String getManifestPath() {
    return manifestPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GcsSourceConfig)) {
      return false;
    }
    return manifestPath.equals(((GcsSourceConfig) o).manifestPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(manifestPath);
  }

  @Override
  public String toString() {
    return "GcsSourceConfig{manifestPath='" + manifestPath + "'}";
  }
}
