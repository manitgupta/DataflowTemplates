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

/** Information about the source system of an export ({@code source} in the manifest). */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class GcsManifestSource implements Serializable {

  @Nullable private final String system;
  @Nullable private final String database;
  @Nullable private final String serverVersion;
  @Nullable private final String exportStartedAt;
  @Nullable private final String exportFinishedAt;
  @Nullable private final Boolean consistent;

  @JsonCreator
  public GcsManifestSource(
      @JsonProperty("system") @Nullable String system,
      @JsonProperty("database") @Nullable String database,
      @JsonProperty("serverVersion") @Nullable String serverVersion,
      @JsonProperty("exportStartedAt") @Nullable String exportStartedAt,
      @JsonProperty("exportFinishedAt") @Nullable String exportFinishedAt,
      @JsonProperty("consistent") @Nullable Boolean consistent) {
    this.system = system;
    this.database = database;
    this.serverVersion = serverVersion;
    this.exportStartedAt = exportStartedAt;
    this.exportFinishedAt = exportFinishedAt;
    this.consistent = consistent;
  }

  /** Source system, for example {@code arangodb} (informational). */
  @Nullable
  public String getSystem() {
    return system;
  }

  /** Source database (informational). */
  @Nullable
  public String getDatabase() {
    return database;
  }

  /** Source server version (informational). */
  @Nullable
  public String getServerVersion() {
    return serverVersion;
  }

  /** ISO-8601 instant at which the export started; used as the read timestamp. */
  @Nullable
  public String getExportStartedAt() {
    return exportStartedAt;
  }

  /** ISO-8601 instant at which the export finished (informational). */
  @Nullable
  public String getExportFinishedAt() {
    return exportFinishedAt;
  }

  /** Whether the exporter observed a consistent source; {@code false} means writes happened. */
  @Nullable
  public Boolean getConsistent() {
    return consistent;
  }
}
