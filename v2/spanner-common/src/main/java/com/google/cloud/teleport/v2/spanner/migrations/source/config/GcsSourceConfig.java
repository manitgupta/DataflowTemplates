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
 * Source config for the GCS source, which reads an Avro export in the bulk migration format (see
 * {@link com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat}); holds only the root
 * directory of the export.
 *
 * <p>The root may be on any Beam FileSystem (gs://, local paths, ...). It is normalized by trimming
 * whitespace and removing trailing {@code /}s. No I/O is performed when the config is created; the
 * export is listed by the source connector.
 */
public final class GcsSourceConfig implements SourceConnectionConfig, Serializable {

  private final String rootPath;

  /**
   * Creates a config pointing to the given export root directory.
   *
   * @param rootPath URI of the root directory of the export; a trailing {@code /} is optional.
   * @throws IllegalArgumentException if {@code rootPath} is null or blank.
   */
  public GcsSourceConfig(String rootPath) {
    String normalized = rootPath == null ? "" : rootPath.trim();
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException(
          "sourceConfigURL must point to the root directory of the Avro export for the GCS"
              + " source.");
    }
    this.rootPath = normalized;
  }

  /**
   * Creates a config from the root directory URI of the export.
   *
   * @param uri URI of the root directory of the export; a trailing {@code /} is optional.
   * @return the config.
   * @throws IllegalArgumentException if {@code uri} is null or blank.
   */
  public static GcsSourceConfig fromUri(String uri) {
    return new GcsSourceConfig(uri);
  }

  /** Returns the URI of the root directory of the export, without trailing {@code /}. */
  public String getRootPath() {
    return rootPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GcsSourceConfig)) {
      return false;
    }
    return rootPath.equals(((GcsSourceConfig) o).rootPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rootPath);
  }

  @Override
  public String toString() {
    return "GcsSourceConfig{rootPath='" + rootPath + "'}";
  }
}
