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
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helpers for {@link GcsIoWrapper}. */
final class GcsIoWrapperHelper {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIoWrapperHelper.class);

  private GcsIoWrapperHelper() {}

  /**
   * Selects the export tables to read (case-insensitive).
   *
   * @param root root directory of the export, for messages.
   * @param exportTables table names found in the export.
   * @param sourceTables requested table names; empty or null means all export tables.
   * @return requested name (or export name when all are selected) to export table name, in order.
   */
  static ImmutableMap<String, String> selectTables(
      String root, Collection<String> exportTables, List<String> sourceTables) {
    Map<String, String> selected = new LinkedHashMap<>();
    if (sourceTables == null || sourceTables.isEmpty()) {
      for (String table : exportTables) {
        selected.put(table, table);
      }
      return ImmutableMap.copyOf(selected);
    }
    Map<String, String> byLowerName = new LinkedHashMap<>();
    for (String table : exportTables) {
      byLowerName.putIfAbsent(table.toLowerCase(Locale.ROOT), table);
    }
    for (String requested : sourceTables) {
      String table = byLowerName.get(requested.toLowerCase(Locale.ROOT));
      if (table == null) {
        LOG.warn("Table {} requested but not present in the Avro export {}", requested, root);
        continue;
      }
      selected.putIfAbsent(requested, table);
    }
    return ImmutableMap.copyOf(selected);
  }

  /**
   * Compares the export tables with the source tables the migration selected (case-insensitive).
   * Logs a warning per export table that will not be migrated, and fails unless {@code
   * tablesConfigured}.
   *
   * @return export table names that will not be migrated, in export order.
   * @throws InvalidOptionsException if some are not migrated and {@code tablesConfigured} is false.
   */
  static List<String> checkAllExportTablesMigrated(
      String root,
      Collection<String> exportTables,
      List<String> selectedSourceTables,
      boolean tablesConfigured) {
    Set<String> selected = new HashSet<>();
    for (String t : selectedSourceTables) {
      if (t != null) {
        selected.add(t.toLowerCase(Locale.ROOT));
      }
    }
    List<String> missing = new ArrayList<>();
    for (String table : exportTables) {
      if (!selected.contains(table.toLowerCase(Locale.ROOT))) {
        missing.add(table);
        LOG.warn(
            "Table {} of the Avro export {} will not be migrated: no matching Spanner table was"
                + " selected",
            table,
            root);
      }
    }
    if (!missing.isEmpty() && !tablesConfigured) {
      throw new InvalidOptionsException(
          String.format(
              "Tables %s of the Avro export %s have no matching Spanner table and would not be"
                  + " migrated. Create them in the Spanner database (see the exporter's generated"
                  + " DDL), map them with a session file / overrides, or restrict the migration"
                  + " explicitly with the 'tables' option.",
              missing, root));
    }
    return missing;
  }

  /**
   * Builds the {@link SourceTableSchema} of an export table. Its payload schema is the files'
   * payload schema field for field, so the template's sink would write the same schema again.
   *
   * @param tableName name of the table in the migration (the requested name).
   * @param table the discovered export table.
   */
  static SourceTableSchema buildTableSchema(String tableName, GcsExportTable table) {
    SourceTableSchema.Builder builder =
        SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
            .setTableName(tableName)
            .setPrimaryKeyColumns(table.primaryKeys());
    for (Schema.Field field : table.payloadSchema().getFields()) {
      builder.addSourceColumnWithAvroSchema(
          field.name(), GcsMappingsProvider.columnType(field.schema()), field.schema());
    }
    return builder.build();
  }
}
