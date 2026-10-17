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

/** A column of a table, as listed in the manifest's {@code tables[].columns[]}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class GcsColumnManifest implements Serializable {

  private final String name;
  private final String type;
  @Nullable private final Boolean nullable;
  @Nullable private final String sourceName;

  @JsonCreator
  public GcsColumnManifest(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("nullable") @Nullable Boolean nullable,
      @JsonProperty("sourceName") @Nullable String sourceName) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.sourceName = sourceName;
  }

  /** Field name in the Avro files and in the row payload. */
  public String getName() {
    return name;
  }

  /** Manifest type of the column (see {@code GcsMappingsProvider.SUPPORTED_TYPES}). */
  public String getType() {
    return type;
  }

  /** Whether the column is nullable (informational). */
  @Nullable
  public Boolean getNullable() {
    return nullable;
  }

  /** Name of the attribute at the source (informational). */
  @Nullable
  public String getSourceName() {
    return sourceName;
  }
}
