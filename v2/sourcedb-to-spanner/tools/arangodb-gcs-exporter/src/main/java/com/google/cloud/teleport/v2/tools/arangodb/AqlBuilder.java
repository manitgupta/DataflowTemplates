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
package com.google.cloud.teleport.v2.tools.arangodb;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import java.util.regex.Pattern;

/**
 * Builds the AQL queries run by the Spark connector in query mode. The connector's query mode has
 * no bind parameters, so collection names are validated and quoted, and key bounds are inlined as
 * JSON-escaped AQL string literals.
 *
 * <p>Every query returns the fixed envelope: {@code key, id, rev, [from_id, to_id,] doc} where
 * {@code doc = JSON_STRINGIFY(d)} is the full document serialised by the server.
 */
public final class AqlBuilder {

  private static final Pattern TRADITIONAL_NAME = Pattern.compile("^[A-Za-z0-9_\\-]+$");

  private AqlBuilder() {}

  /**
   * HASH partition query: {@code FILTER HASH(d._key) % partitions == index}, or no filter when
   * {@code partitions == 1}.
   */
  public static String partitionQuery(CollectionInfo c, int partitions, int index) {
    return exportQuery(c, hashFilter(c, partitions, index));
  }

  /**
   * RANGE partition query over {@code [range.lo, range.hi)}; no filter for {@link KeyRange#ALL}.
   */
  public static String rangeQuery(CollectionInfo c, KeyRange range) {
    return exportQuery(c, rangeFilter(range));
  }

  /**
   * The export query for a filter clause produced by {@link #hashFilter} or {@link #rangeFilter}.
   */
  public static String exportQuery(CollectionInfo c, String filter) {
    return "FOR d IN " + quoteCollection(c.sourceName()) + " " + filter + returnClause(c.kind());
  }

  /**
   * A query returning the number of documents matched by a filter clause, with exactly the same
   * {@code FOR}/{@code FILTER} text as {@link #exportQuery}. Used to verify that partitions are
   * disjoint and complete.
   */
  public static String countQuery(CollectionInfo c, String filter) {
    return "FOR d IN "
        + quoteCollection(c.sourceName())
        + " "
        + filter
        + "COLLECT WITH COUNT INTO n RETURN n";
  }

  /** {@code "FILTER HASH(d._key) % N == i "} or {@code ""} when {@code partitions == 1}. */
  static String hashFilter(CollectionInfo c, int partitions, int index) {
    if (partitions < 1 || index < 0 || index >= partitions) {
      throw new IllegalArgumentException(
          "Invalid partition " + index + " of " + partitions + " for " + c.sourceName());
    }
    return partitions == 1 ? "" : "FILTER HASH(d._key) % " + partitions + " == " + index + " ";
  }

  /** Key range filter clause (with trailing space), or {@code ""} for {@link KeyRange#ALL}. */
  static String rangeFilter(KeyRange range) {
    StringBuilder filter = new StringBuilder();
    if (range.lo() != null && range.hi() != null) {
      filter
          .append("FILTER d._key >= ")
          .append(aqlString(range.lo()))
          .append(" AND d._key < ")
          .append(aqlString(range.hi()))
          .append(' ');
    } else if (range.lo() != null) {
      filter.append("FILTER d._key >= ").append(aqlString(range.lo())).append(' ');
    } else if (range.hi() != null) {
      filter.append("FILTER d._key < ").append(aqlString(range.hi())).append(' ');
    }
    return filter.toString();
  }

  private static String returnClause(CollectionKind kind) {
    if (kind == CollectionKind.EDGE) {
      return "RETURN {key: d._key, id: d._id, rev: d._rev, from_id: d._from, to_id: d._to,"
          + " doc: JSON_STRINGIFY(d)}";
    }
    return "RETURN {key: d._key, id: d._id, rev: d._rev, doc: JSON_STRINGIFY(d)}";
  }

  /**
   * Validates a traditional ArangoDB collection name ({@code ^[A-Za-z0-9_-]+$}) and wraps it in
   * backticks.
   */
  static String quoteCollection(String name) {
    if (name == null || !TRADITIONAL_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Unsupported collection name '"
              + name
              + "': only traditional ArangoDB names ([A-Za-z0-9_-]) are supported.");
    }
    return "`" + name + "`";
  }

  /** A double-quoted AQL string literal, escaped with a JSON string encoder. */
  static String aqlString(String value) {
    return "\"" + new String(JsonStringEncoder.getInstance().quoteAsString(value)) + "\"";
  }
}
