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
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** A table of the export, as listed in the manifest's {@code tables[]}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class GcsTableManifest implements Serializable {

  private static final Pattern GLOB_CHARS = Pattern.compile("[*?\\[\\]{}]");

  private final String name;
  @Nullable private final String sourceName;
  @Nullable private final String collectionType;
  private final ImmutableList<String> primaryKey;
  private final ImmutableList<GcsColumnManifest> columns;
  @Nullable private final ImmutableList<GcsFileManifest> files;
  @Nullable private final String filePattern;
  @Nullable private final Long rowCount;

  @JsonCreator
  public GcsTableManifest(
      @JsonProperty("name") String name,
      @JsonProperty("sourceName") @Nullable String sourceName,
      @JsonProperty("collectionType") @Nullable String collectionType,
      @JsonProperty("primaryKey") @Nullable List<String> primaryKey,
      @JsonProperty("columns") @Nullable List<GcsColumnManifest> columns,
      @JsonProperty("files") @Nullable List<GcsFileManifest> files,
      @JsonProperty("filePattern") @Nullable String filePattern,
      @JsonProperty("rowCount") @Nullable Long rowCount) {
    this.name = name;
    this.sourceName = sourceName;
    this.collectionType = collectionType;
    this.primaryKey = primaryKey == null ? ImmutableList.of() : ImmutableList.copyOf(primaryKey);
    this.columns = columns == null ? ImmutableList.of() : ImmutableList.copyOf(columns);
    this.files = files == null ? null : ImmutableList.copyOf(files);
    this.filePattern = filePattern;
    this.rowCount = rowCount;
  }

  /** Table name seen by the schema mapper. */
  public String getName() {
    return name;
  }

  /** Original name at the source, for example the ArangoDB collection name. */
  @Nullable
  public String getSourceName() {
    return sourceName;
  }

  /** {@code DOCUMENT} or {@code EDGE} for ArangoDB exports (informational). */
  @Nullable
  public String getCollectionType() {
    return collectionType;
  }

  /** Primary key columns (informational; Spanner uses the DDL primary key). */
  public ImmutableList<String> getPrimaryKey() {
    return primaryKey;
  }

  /** Columns in manifest order. */
  public ImmutableList<GcsColumnManifest> getColumns() {
    return columns;
  }

  /** Explicit list of data files, or {@code null} when {@link #getFilePattern()} is used. */
  @Nullable
  public ImmutableList<GcsFileManifest> getFiles() {
    return files;
  }

  /** Glob matching the data files, or {@code null} when {@link #getFiles()} is used. */
  @Nullable
  public String getFilePattern() {
    return filePattern;
  }

  /** Expected total number of rows, if known. */
  @Nullable
  public Long getRowCount() {
    return rowCount;
  }

  /**
   * Returns the data file URIs (or the single glob) of this table, resolved against the manifest's
   * directory.
   *
   * <p>Resolved files are matched as Beam file specs, which treat {@code * ? [ ] { }} as glob
   * syntax. An explicit file whose resolved URI (including the manifest's directory) contains one
   * of them is rejected rather than silently matching other objects or nothing.
   *
   * @param manifestPath URI of the manifest.
   * @throws IllegalArgumentException if a resolved explicit file contains a glob metacharacter.
   */
  public List<String> resolveFilePatterns(String manifestPath) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    if (files != null && !files.isEmpty()) {
      for (GcsFileManifest file : files) {
        String resolved = GcsManifestPaths.resolve(manifestPath, file.getPath());
        if (GLOB_CHARS.matcher(resolved).find()) {
          throw new IllegalArgumentException(
              "Table "
                  + name
                  + " lists file "
                  + resolved
                  + " which contains a glob metacharacter (* ? [ ] { }); move the export to a"
                  + " path without these characters");
        }
        builder.add(resolved);
      }
    } else if (filePattern != null) {
      builder.add(GcsManifestPaths.resolve(manifestPath, filePattern));
    }
    return builder.build();
  }
}
