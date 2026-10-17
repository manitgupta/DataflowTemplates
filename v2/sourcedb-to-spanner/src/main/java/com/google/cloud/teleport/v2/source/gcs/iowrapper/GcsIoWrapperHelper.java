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
package com.google.cloud.teleport.v2.source.gcs.iowrapper;

import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsColumnManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsExportManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsTableManifest;
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helpers for {@link GcsIoWrapper}. */
final class GcsIoWrapperHelper {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIoWrapperHelper.class);

  private GcsIoWrapperHelper() {}

  /**
   * Selects the manifest tables to read.
   *
   * @param manifest the export manifest.
   * @param sourceTables requested table names; empty means all manifest tables.
   * @return requested name (or manifest name when all are selected) to manifest table, in order.
   */
  static ImmutableMap<String, GcsTableManifest> selectTables(
      GcsExportManifest manifest, List<String> sourceTables) {
    Map<String, GcsTableManifest> selected = new LinkedHashMap<>();
    if (sourceTables == null || sourceTables.isEmpty()) {
      for (GcsTableManifest table : manifest.getTables()) {
        selected.put(table.getName(), table);
      }
      return ImmutableMap.copyOf(selected);
    }
    Map<String, GcsTableManifest> byLowerName = new LinkedHashMap<>();
    for (GcsTableManifest table : manifest.getTables()) {
      byLowerName.put(table.getName().toLowerCase(Locale.ROOT), table);
    }
    for (String requested : sourceTables) {
      GcsTableManifest table = byLowerName.get(requested.toLowerCase(Locale.ROOT));
      if (table == null) {
        LOG.warn(
            "Table {} requested but not present in manifest {}",
            requested,
            manifest.exportIdOrDefault());
        continue;
      }
      selected.putIfAbsent(requested, table);
    }
    return ImmutableMap.copyOf(selected);
  }

  /**
   * Compares the manifest tables with the source tables the migration selected (case-insensitive).
   * Logs a warning per manifest table that will not be migrated, and fails unless {@code
   * tablesConfigured}.
   *
   * @return manifest table names that will not be migrated, in manifest order.
   * @throws InvalidOptionsException if some are not migrated and {@code tablesConfigured} is false.
   */
  static List<String> checkAllManifestTablesMigrated(
      GcsExportManifest manifest, List<String> selectedSourceTables, boolean tablesConfigured) {
    Set<String> selected = new HashSet<>();
    for (String t : selectedSourceTables) {
      if (t != null) {
        selected.add(t.toLowerCase(Locale.ROOT));
      }
    }
    List<String> missing = new ArrayList<>();
    for (GcsTableManifest table : manifest.getTables()) {
      if (!selected.contains(table.getName().toLowerCase(Locale.ROOT))) {
        missing.add(table.getName());
        LOG.warn(
            "Manifest table {} of export {} will not be migrated: no matching Spanner table was"
                + " selected",
            table.getName(),
            manifest.exportIdOrDefault());
      }
    }
    if (!missing.isEmpty() && !tablesConfigured) {
      throw new InvalidOptionsException(
          String.format(
              "Manifest tables %s of export %s have no matching Spanner table and would not be"
                  + " migrated. Create them in the Spanner database (see the exporter's generated"
                  + " DDL), map them with a session file / overrides, or restrict the migration"
                  + " explicitly with the 'tables' option.",
              missing, manifest.exportIdOrDefault()));
    }
    return missing;
  }

  /** Builds the {@link SourceTableSchema} of a manifest table, columns in manifest order. */
  static SourceTableSchema buildTableSchema(String tableName, GcsTableManifest table) {
    SourceTableSchema.Builder builder =
        SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
            .setTableName(tableName)
            .setPrimaryKeyColumns(ImmutableList.copyOf(table.getPrimaryKey()));
    for (GcsColumnManifest column : table.getColumns()) {
      builder.addSourceColumnNameToSourceColumnType(
          column.getName(),
          new SourceColumnType(
              column.getType().toUpperCase(Locale.ROOT), new Long[] {}, new Long[] {}));
    }
    return builder.build();
  }

  /** Returns column name to upper-case manifest type, in manifest order. */
  static ImmutableMap<String, String> columnTypes(GcsTableManifest table) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (GcsColumnManifest column : table.getColumns()) {
      builder.put(column.getName(), column.getType().toUpperCase(Locale.ROOT));
    }
    return builder.build();
  }
}
