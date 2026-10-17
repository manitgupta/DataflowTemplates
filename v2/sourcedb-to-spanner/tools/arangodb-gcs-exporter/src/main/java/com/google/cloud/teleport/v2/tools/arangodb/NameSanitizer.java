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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Maps ArangoDB collection names to Spanner/Avro-legal table names: lower case, every character
 * outside {@code [a-z0-9_]} replaced by {@code _}, a {@code t_} prefix when the first character is
 * not a letter, at most 128 characters.
 */
public final class NameSanitizer {

  static final int MAX_LENGTH = 128;

  private NameSanitizer() {}

  /** Sanitizes one collection name. */
  public static String toTableName(String collectionName) {
    if (collectionName == null || collectionName.isEmpty()) {
      throw new IllegalArgumentException("Collection name must not be empty");
    }
    String lower = collectionName.toLowerCase(Locale.ROOT);
    StringBuilder sb = new StringBuilder(lower.length() + 2);
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      boolean legal = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
      sb.append(legal ? c : '_');
    }
    char first = sb.charAt(0);
    if (!(first >= 'a' && first <= 'z')) {
      sb.insert(0, "t_");
    }
    if (sb.length() > MAX_LENGTH) {
      sb.setLength(MAX_LENGTH);
    }
    return sb.toString();
  }

  /**
   * Sanitizes all names, preserving input order.
   *
   * @return collection name to table name
   * @throws IllegalArgumentException if two collections map to the same table name
   *     (case-insensitive)
   */
  public static Map<String, String> toTableNames(List<String> collections) {
    Map<String, String> result = new LinkedHashMap<>();
    Map<String, String> byTable = new HashMap<>();
    for (String collection : collections) {
      String table = toTableName(collection);
      String key = table.toLowerCase(Locale.ROOT);
      String previous = byTable.putIfAbsent(key, collection);
      if (previous != null && !previous.equals(collection)) {
        throw new IllegalArgumentException(
            "Collections '"
                + previous
                + "' and '"
                + collection
                + "' both map to table name '"
                + table
                + "'. Export them separately (--collections / --excludeCollections).");
      }
      result.put(collection, table);
    }
    return result;
  }
}
