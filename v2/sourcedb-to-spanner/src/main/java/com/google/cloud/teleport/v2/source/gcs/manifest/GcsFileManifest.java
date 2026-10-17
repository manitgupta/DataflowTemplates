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
package com.google.cloud.teleport.v2.source.gcs.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import javax.annotation.Nullable;

/** A data file of a table, as listed in the manifest's {@code tables[].files[]}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class GcsFileManifest implements Serializable {

  private final String path;
  @Nullable private final Long rows;

  @JsonCreator
  public GcsFileManifest(
      @JsonProperty("path") String path, @JsonProperty("rows") @Nullable Long rows) {
    this.path = path;
    this.rows = rows;
  }

  /** Path of the file, relative to the manifest's directory unless absolute. */
  public String getPath() {
    return path;
  }

  /** Number of rows in the file, if known. */
  @Nullable
  public Long getRows() {
    return rows;
  }
}
