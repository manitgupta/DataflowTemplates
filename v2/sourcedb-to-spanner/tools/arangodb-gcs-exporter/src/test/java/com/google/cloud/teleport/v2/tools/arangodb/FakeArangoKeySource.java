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
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Simulates the ArangoDB behaviour relevant to RANGE partitioning: AQL string comparison uses an
 * ICU-like collation (punctuation before digits before letters, case-insensitive first with lower
 * case before upper case), the primary index range scan compares bytes, and the optimizer folds a
 * two-sided range to EMPTY when {@code lo >= hi} in collation order.
 */
final class FakeArangoKeySource implements KeyRangePlanner.KeySource {

  /** Primary order of non-letter characters, roughly as in the ICU root collation. */
  private static final String PRIMARY_ORDER = "_-,;:!.'()@*%+=$0123456789";

  /** ICU-like collation over legal ArangoDB key characters. */
  static final Comparator<String> COLLATION =
      (a, b) -> {
        int n = Math.min(a.length(), b.length());
        for (int i = 0; i < n; i++) {
          int c = Integer.compare(primary(a.charAt(i)), primary(b.charAt(i)));
          if (c != 0) {
            return c;
          }
        }
        if (a.length() != b.length()) {
          return Integer.compare(a.length(), b.length());
        }
        for (int i = 0; i < n; i++) {
          int c = Integer.compare(tertiary(a.charAt(i)), tertiary(b.charAt(i)));
          if (c != 0) {
            return c;
          }
        }
        return 0;
      };

  private static int primary(char ch) {
    int idx = PRIMARY_ORDER.indexOf(ch);
    if (idx >= 0) {
      return idx;
    }
    if (Character.isLetter(ch)) {
      return 100 + Character.toLowerCase(ch);
    }
    return 1000 + ch;
  }

  private static int tertiary(char ch) {
    return Character.isUpperCase(ch) ? 1 : 0;
  }

  private final List<String> keys;
  private final long seed;
  int sampleCalls;
  int sortCalls;
  int lessCalls;

  /** Pairs (by value) the fake server claims are NOT ordered, to exercise the verification step. */
  private final List<String[]> disagreements = new ArrayList<>();

  FakeArangoKeySource(List<String> keys, long seed) {
    this.keys = keys;
    this.seed = seed;
  }

  void disagreeOn(String lo, String hi) {
    disagreements.add(new String[] {lo, hi});
  }

  @Override
  public List<String> sampleKeys(String collection, double probability) {
    sampleCalls++;
    if (probability >= 1.0) {
      return new ArrayList<>(keys);
    }
    Random r = new Random(seed);
    List<String> out = new ArrayList<>();
    for (String k : keys) {
      if (r.nextDouble() < probability) {
        out.add(k);
      }
    }
    return out;
  }

  @Override
  public List<String> collationSort(List<String> values) {
    sortCalls++;
    List<String> sorted = new ArrayList<>(values);
    sorted.sort(COLLATION);
    return sorted;
  }

  @Override
  public List<Boolean> collationLessThanPairs(List<String> values) {
    lessCalls++;
    List<Boolean> out = new ArrayList<>();
    for (int i = 0; i + 1 < values.size(); i++) {
      boolean less = COLLATION.compare(values.get(i), values.get(i + 1)) < 0;
      for (String[] d : disagreements) {
        if (d[0].equals(values.get(i)) && d[1].equals(values.get(i + 1))) {
          less = false;
        }
      }
      out.add(less);
    }
    return out;
  }

  /** Rows the simulated server returns for a range query. */
  long countRange(KeyRange r) {
    if (r.lo() != null && r.hi() != null && COLLATION.compare(r.lo(), r.hi()) >= 0) {
      return 0; // optimizer folds the constant range to EMPTY
    }
    long n = 0;
    for (String k : keys) {
      if (inByteRange(k, r)) {
        n++;
      }
    }
    return n;
  }

  static boolean inByteRange(String k, KeyRange r) {
    return (r.lo() == null || KeyRangePlanner.UTF8_BYTE_ORDER.compare(k, r.lo()) >= 0)
        && (r.hi() == null || KeyRangePlanner.UTF8_BYTE_ORDER.compare(k, r.hi()) < 0);
  }
}
