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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;

/**
 * The {@code manifest.json} of a GCS file export (format version {@value
 * #SUPPORTED_FORMAT_VERSION}). Unknown JSON fields are ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class GcsExportManifest implements Serializable {

  /** The only manifest format version understood by this reader. */
  public static final int SUPPORTED_FORMAT_VERSION = 1;

  /** The only supported data file format. */
  public static final String SUPPORTED_FILE_FORMAT = "AVRO";

  /** Default export id used when the manifest does not carry one. */
  public static final String DEFAULT_EXPORT_ID = "export";

  private static final Pattern AVRO_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final int formatVersion;
  @Nullable private final String fileFormat;
  @Nullable private final String exportId;
  @Nullable private final String producer;
  @Nullable private final GcsManifestSource source;
  private final ImmutableList<GcsTableManifest> tables;

  @JsonCreator
  public GcsExportManifest(
      @JsonProperty("formatVersion") int formatVersion,
      @JsonProperty("fileFormat") @Nullable String fileFormat,
      @JsonProperty("exportId") @Nullable String exportId,
      @JsonProperty("producer") @Nullable String producer,
      @JsonProperty("source") @Nullable GcsManifestSource source,
      @JsonProperty("tables") @Nullable List<GcsTableManifest> tables) {
    this.formatVersion = formatVersion;
    this.fileFormat = fileFormat;
    this.exportId = exportId;
    this.producer = producer;
    this.source = source;
    this.tables = tables == null ? ImmutableList.of() : ImmutableList.copyOf(tables);
  }

  /**
   * Reads, parses and validates the manifest at the given URI (any Beam FileSystem).
   *
   * @throws UncheckedIOException if the manifest cannot be read.
   * @throws IllegalArgumentException if the manifest is invalid.
   */
  public static GcsExportManifest read(String manifestPath) {
    try (InputStream in =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchSingleFileSpec(manifestPath).resourceId()))) {
      return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read GCS export manifest " + manifestPath, e);
    }
  }

  /**
   * Parses and validates a manifest.
   *
   * @throws IllegalArgumentException if the JSON is malformed or the manifest is invalid.
   */
  public static GcsExportManifest parse(String json) {
    GcsExportManifest manifest;
    try {
      manifest = MAPPER.readValue(json, GcsExportManifest.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Malformed GCS export manifest: " + e.getMessage(), e);
    }
    if (manifest == null) {
      throw new IllegalArgumentException("Malformed GCS export manifest: empty document");
    }
    manifest.validate();
    return manifest;
  }

  /**
   * Validates the manifest.
   *
   * @throws IllegalArgumentException describing the first violated rule.
   */
  @VisibleForTesting
  void validate() {
    if (formatVersion != SUPPORTED_FORMAT_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported manifest formatVersion "
              + formatVersion
              + "; only "
              + SUPPORTED_FORMAT_VERSION
              + " is supported");
    }
    if (!SUPPORTED_FILE_FORMAT.equalsIgnoreCase(fileFormat)) {
      throw new IllegalArgumentException(
          "Unsupported manifest fileFormat " + fileFormat + "; only AVRO is supported");
    }
    if (source != null && !isBlank(source.getExportStartedAt())) {
      try {
        Instant.parse(source.getExportStartedAt());
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(
            "Invalid source.exportStartedAt " + source.getExportStartedAt(), e);
      }
    }
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("Manifest must list at least one table");
    }
    Set<String> tableNames = new HashSet<>();
    for (GcsTableManifest table : tables) {
      validateTable(table, tableNames);
    }
  }

  private static void validateTable(GcsTableManifest table, Set<String> tableNames) {
    String name = table.getName();
    if (isBlank(name)) {
      throw new IllegalArgumentException("Manifest table name must not be blank");
    }
    if (!AVRO_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Manifest table name " + name + " must match " + AVRO_NAME.pattern());
    }
    if (!tableNames.add(name.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException(
          "Duplicate manifest table name (case-insensitive): " + name);
    }
    if (table.getColumns().isEmpty()) {
      throw new IllegalArgumentException("Table " + name + " must have at least one column");
    }
    Set<String> columnNames = new HashSet<>();
    for (GcsColumnManifest column : table.getColumns()) {
      String columnName = column.getName();
      if (isBlank(columnName) || !AVRO_NAME.matcher(columnName).matches()) {
        throw new IllegalArgumentException(
            "Table " + name + " column name " + columnName + " must match " + AVRO_NAME.pattern());
      }
      if (!columnNames.add(columnName)) {
        throw new IllegalArgumentException("Table " + name + " has duplicate column " + columnName);
      }
      if (column.getType() == null
          || !GcsMappingsProvider.SUPPORTED_TYPES.contains(
              column.getType().toUpperCase(Locale.ROOT))) {
        throw new IllegalArgumentException(
            "Table "
                + name
                + " column "
                + columnName
                + " has unsupported type "
                + column.getType()
                + "; supported types are "
                + GcsMappingsProvider.SUPPORTED_TYPES);
      }
    }
    for (String pk : table.getPrimaryKey()) {
      if (!columnNames.contains(pk)) {
        throw new IllegalArgumentException(
            "Table " + name + " primary key column " + pk + " is not a column of the table");
      }
    }
    boolean hasFiles = table.getFiles() != null && !table.getFiles().isEmpty();
    boolean hasPattern = !isBlank(table.getFilePattern());
    if (hasFiles == hasPattern) {
      throw new IllegalArgumentException(
          "Table " + name + " must have exactly one of a non-empty 'files' or a 'filePattern'");
    }
    if (hasFiles) {
      for (GcsFileManifest file : table.getFiles()) {
        if (file == null || isBlank(file.getPath())) {
          throw new IllegalArgumentException("Table " + name + " lists a file with a blank path");
        }
      }
    }
  }

  private static boolean isBlank(@Nullable String s) {
    return s == null || s.trim().isEmpty();
  }

  /** Manifest format version. */
  public int getFormatVersion() {
    return formatVersion;
  }

  /** Data file format; always {@code AVRO} for a valid manifest. */
  @Nullable
  public String getFileFormat() {
    return fileFormat;
  }

  /** Export id, if present. */
  @Nullable
  public String getExportId() {
    return exportId;
  }

  /** Producer of the export (informational). */
  @Nullable
  public String getProducer() {
    return producer;
  }

  /** Source system information, if present. */
  @Nullable
  public GcsManifestSource getSource() {
    return source;
  }

  /** Tables of the export, in manifest order. */
  public ImmutableList<GcsTableManifest> getTables() {
    return tables;
  }

  /** Returns the export id, or {@value #DEFAULT_EXPORT_ID} if absent or blank. */
  public String exportIdOrDefault() {
    return isBlank(exportId) ? DEFAULT_EXPORT_ID : exportId;
  }

  /**
   * Returns the read timestamp of all rows of this export in epoch microseconds: {@code
   * source.exportStartedAt} when present, else the current time.
   */
  public long readTimeMicros() {
    if (source != null && !isBlank(source.getExportStartedAt())) {
      Instant instant = Instant.parse(source.getExportStartedAt());
      return Math.addExact(
          Math.multiplyExact(instant.getEpochSecond(), TimeUnit.SECONDS.toMicros(1)),
          TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
    }
    return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
  }
}
