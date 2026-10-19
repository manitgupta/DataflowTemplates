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
package com.google.cloud.teleport.v2.spanner.migrations.gcs;

import com.google.common.base.CharMatcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * The Avro file layout written by the bulk migration template ({@code sourcedb-to-spanner} with
 * {@code gcsOutputDirectory}) and read by the GCS validation template and the template's GCS
 * source.
 *
 * <p>Files are laid out as {@code
 * root/<tableName>/[<shardId>/]<md5Hex(schema)>-NNNNN-of-MMMMM.avro}. Every record has the schema
 * {@value #RECORD_NAME} {@code {tableName: string, shardId: ["string","null"], primaryKeys:
 * array<string>, payload: record "payload" <one nullable field per source column>}}.
 *
 * <p>Which files get listed depends on the table and shard filters:
 *
 * <ul>
 *   <li>No filters: one pattern, root/**.avro. Every file is read.
 *   <li>Tables only: one pattern per table, root/table/**.avro.
 *   <li>Tables and shards: one pattern per table and shard, root/table/shardId/**.avro.
 *   <li>Shards only: one pattern, root/**.avro, then {@link #isConfiguredFile} keeps the selected
 *       shards. A per-shard pattern with a wildcard table folder would list all of root once per
 *       shard.
 *   <li>A table name or shard ID containing * ? [ or \: same as shards only. Beam globs can't
 *       escape these characters, so such a name can't be written in a pattern.
 * </ul>
 *
 * <p>An empty (or null) table or shard collection means "no filter".
 */
public final class BulkGcsFormat {

  /** Record field holding the source table name. */
  public static final String TABLE_NAME_FIELD = "tableName";

  /** Record field holding the logical shard id, null for non-sharded migrations. */
  public static final String SHARD_ID_FIELD = "shardId";

  /** Record field holding the primary key column names of the source table. */
  public static final String PRIMARY_KEYS_FIELD = "primaryKeys";

  /** Record field holding the source row, one field per source column. */
  public static final String PAYLOAD_FIELD = "payload";

  /** Name of the top-level Avro record. */
  public static final String RECORD_NAME = "SourceRowWithMetadata";

  private static final CharMatcher GLOB_CHARS = CharMatcher.anyOf("*?[\\");

  private BulkGcsFormat() {}

  /**
   * Returns the Beam file patterns that list the files of the selected tables and shards.
   *
   * @param root root directory of the export; a trailing {@code /} is ignored.
   * @param tables selected table names; empty or null means all tables.
   * @param shardIds selected shard ids; empty or null means all shards.
   */
  public static List<String> filePatterns(
      String root, @Nullable Collection<String> tables, @Nullable Collection<String> shardIds) {
    String base = stripTrailingSlash(root);
    Collection<String> t = orEmpty(tables);
    Collection<String> s = orEmpty(shardIds);
    // Without a table filter, or with a name Beam globs can't express, list root once.
    // A shard-only filter root/*/shard pattern would re-list all of root for every shard, so this
    // is handled by the filtering flow as well.
    if (t.isEmpty() || hasGlobChar(t, s)) {
      return List.of(base + "/**.avro");
    }
    List<String> patterns = new ArrayList<>();
    for (String table : t) {
      if (s.isEmpty()) {
        patterns.add(base + "/" + table + "/**.avro");
      } else {
        for (String shardId : s) {
          // The '/' after the shard ID keeps shard_1 from matching shard_10.
          patterns.add(base + "/" + table + "/" + shardId + "/**.avro");
        }
      }
    }
    return patterns;
  }

  /**
   * True iff {@link #filePatterns} lists all of root while tables or shards are selected, so the
   * matched files must be filtered with {@link #isConfiguredFile}: shards-only runs, and names
   * containing glob characters.
   */
  public static boolean needsFileFilter(
      @Nullable Collection<String> tables, @Nullable Collection<String> shardIds) {
    Collection<String> t = orEmpty(tables);
    Collection<String> s = orEmpty(shardIds);
    return (!s.isEmpty() && t.isEmpty()) || hasGlobChar(t, s);
  }

  /**
   * True iff {@code path} (root/table/[shard/]file.avro) is under a selected table and, when shards
   * are selected, a selected shard.
   *
   * @param root root directory of the export; a trailing {@code /} is ignored.
   * @param tables selected table names; empty or null means all tables.
   * @param shardIds selected shard ids; empty or null means all shards.
   * @param path full path of a file under {@code root}.
   */
  public static boolean isConfiguredFile(
      String root,
      @Nullable Collection<String> tables,
      @Nullable Collection<String> shardIds,
      String path) {
    Collection<String> t = orEmpty(tables);
    Collection<String> s = orEmpty(shardIds);
    String[] segments = path.substring(stripTrailingSlash(root).length() + 1).split("/", 3);
    if (!t.isEmpty() && !t.contains(segments[0])) {
      return false;
    }
    return s.isEmpty() || (segments.length == 3 && s.contains(segments[1]));
  }

  /** Returns {@code path} without one trailing {@code /}. */
  public static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  /** True iff a table name or shard ID contains a character Beam globs can't escape. */
  private static boolean hasGlobChar(Collection<String> tables, Collection<String> shardIds) {
    return Stream.concat(tables.stream(), shardIds.stream()).anyMatch(GLOB_CHARS::matchesAnyOf);
  }

  private static Collection<String> orEmpty(@Nullable Collection<String> values) {
    return values == null ? Collections.emptyList() : values;
  }
}
