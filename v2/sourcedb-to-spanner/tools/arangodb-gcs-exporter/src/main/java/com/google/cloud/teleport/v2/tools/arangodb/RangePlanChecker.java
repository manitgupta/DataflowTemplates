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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Checks the execution plans of RANGE partition queries. RANGE partitioning is only complete and
 * disjoint if every range query is answered by a primary-index range scan (byte order) with the
 * FILTER fully covered by the index: a residual {@code FilterNode} would re-check the bounds with
 * AQL collation order and silently drop keys, and a full collection scan compares with collation
 * order too.
 */
public final class RangePlanChecker {

  private RangePlanChecker() {}

  /**
   * The queries to explain for a collection: the first (one-sided {@code <}), a middle and the last
   * (one-sided {@code >=}) range query. Returns an empty list for a single query.
   */
  static List<String> sampleQueries(List<String> queries) {
    Set<String> picked = new LinkedHashSet<>();
    if (queries.size() > 1) {
      picked.add(queries.get(0));
      picked.add(queries.get(queries.size() / 2));
      picked.add(queries.get(queries.size() - 1));
    }
    return new ArrayList<>(picked);
  }

  /**
   * Problems of one explained plan (empty if OK).
   *
   * @param nodes plan nodes as maps with {@code type}, and for index nodes {@code indexes} and
   *     {@code filter} (a post-filter the optimizer moved into the index node)
   */
  static List<String> problems(List<Map<String, Object>> nodes) {
    List<String> problems = new ArrayList<>();
    boolean primaryIndexNode = false;
    for (Map<String, Object> node : nodes) {
      Object type = node.get("type");
      if ("FilterNode".equals(type)) {
        problems.add("a FilterNode remains (bounds would be re-checked in AQL collation order)");
      } else if ("EnumerateCollectionNode".equals(type)) {
        problems.add("an EnumerateCollectionNode (full scan) is used instead of the primary index");
      } else if ("IndexNode".equals(type) && usesPrimaryIndex(node.get("indexes"))) {
        primaryIndexNode = true;
        if (node.get("filter") != null) {
          problems.add(
              "the primary IndexNode has a post-filter (bounds would be re-checked in AQL"
                  + " collation order)");
        }
      }
    }
    if (!primaryIndexNode) {
      problems.add("no IndexNode on the primary index");
    }
    return problems;
  }

  private static boolean usesPrimaryIndex(Object indexes) {
    if (!(indexes instanceof Collection)) {
      return false;
    }
    for (Object index : (Collection<?>) indexes) {
      if (index instanceof Map && "primary".equals(((Map<?, ?>) index).get("type"))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Explains the first, a middle and the last query and fails if any plan is not a pure
   * primary-index range scan.
   *
   * @param explain query -> plan nodes (see {@link ArangoMetadataClient#explainNodes})
   * @throws IllegalStateException with every problem found
   */
  static void check(
      String collection,
      List<String> queries,
      Function<String, List<Map<String, Object>>> explain) {
    List<String> all = new ArrayList<>();
    for (String q : sampleQueries(queries)) {
      for (String p : problems(explain.apply(q))) {
        all.add(p + " in: " + q);
      }
    }
    if (!all.isEmpty()) {
      throw new IllegalStateException(
          collection
              + ": RANGE partition queries are not pure primary-index range scans, so ranges could"
              + " drop or duplicate keys: "
              + String.join("; ", all)
              + ". Use --partitioning=HASH for this collection, or (at your own risk)"
              + " --verifyRangePlans=false.");
    }
  }
}
