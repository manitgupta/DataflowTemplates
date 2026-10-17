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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans primary-key ranges for {@link PartitioningMode#RANGE}.
 *
 * <p>ArangoDB serves {@code d._key >= lo AND d._key < hi} from the primary index, comparing bytes
 * at run time, but the AQL optimizer folds constant bounds using AQL string collation (where {@code
 * 'B' < 'a'} is false). A range is therefore only reliable if {@code lo < hi} in BOTH byte order
 * and collation order. The planner:
 *
 * <ol>
 *   <li>samples about {@code 20 * n} keys (min 1,000, max 2,000,000) with an index-only {@code
 *       RAND()} filter,
 *   <li>sorts them by UTF-8 bytes, de-duplicates and takes {@code n - 1} evenly spaced quantiles,
 *   <li>ranks the quantiles by AQL collation ({@code SORT}) and keeps the longest subsequence whose
 *       collation ranks are strictly increasing (LIS),
 *   <li>double-checks consecutive boundaries with the server's own {@code <} operator and drops any
 *       remaining offender,
 *   <li>emits {@code (-inf, b0)}, {@code [b0, b1)}, ..., {@code [bLast, +inf)}.
 * </ol>
 *
 * Because the boundaries are strictly increasing in byte order, the ranges are disjoint and cover
 * every key regardless of how many boundaries were dropped; dropping only makes ranges larger.
 */
public final class KeyRangePlanner {

  private static final Logger LOG = LoggerFactory.getLogger(KeyRangePlanner.class);

  static final int SAMPLES_PER_PARTITION = 20;
  static final int MIN_SAMPLES = 1_000;
  static final int MAX_SAMPLES = 2_000_000;

  /** Server operations needed by the planner; implemented by {@link ArangoMetadataClient}. */
  public interface KeySource {
    /**
     * Returns the {@code _key} of documents selected with {@code RAND() < probability} (all keys
     * when {@code probability >= 1}).
     */
    List<String> sampleKeys(String collection, double probability);

    /** Returns {@code values} sorted by AQL ({@code FOR b IN @b SORT b RETURN b}). */
    List<String> collationSort(List<String> values);

    /**
     * For {@code values} of size {@code m >= 2}, returns {@code m - 1} flags where flag {@code i}
     * is the AQL result of {@code values[i] < values[i + 1]}.
     */
    List<Boolean> collationLessThanPairs(List<String> values);
  }

  /** Unsigned lexicographic comparison of UTF-8 encodings (ArangoDB primary index order). */
  public static final Comparator<String> UTF8_BYTE_ORDER =
      (a, b) -> {
        byte[] x = a.getBytes(StandardCharsets.UTF_8);
        byte[] y = b.getBytes(StandardCharsets.UTF_8);
        int n = Math.min(x.length, y.length);
        for (int i = 0; i < n; i++) {
          int c = Integer.compare(x[i] & 0xff, y[i] & 0xff);
          if (c != 0) {
            return c;
          }
        }
        return Integer.compare(x.length, y.length);
      };

  private KeyRangePlanner() {}

  /** Plans up to {@code n} ranges for a collection with about {@code count} documents. */
  public static List<KeyRange> plan(KeySource source, String collection, long count, int n) {
    if (n <= 1 || count <= 0) {
      return Collections.singletonList(KeyRange.ALL);
    }
    long wanted = Math.max(MIN_SAMPLES, Math.min(MAX_SAMPLES, (long) n * SAMPLES_PER_PARTITION));
    double probability = Math.min(1.0, (double) wanted / (double) count);
    List<String> samples = source.sampleKeys(collection, probability);
    List<String> sorted = byteSortedDistinct(samples);
    List<String> candidates = quantiles(sorted, n);
    if (candidates.isEmpty()) {
      LOG.info("{}: {} sampled keys, single range", collection, samples.size());
      return Collections.singletonList(KeyRange.ALL);
    }
    List<String> collationSorted = source.collationSort(candidates);
    List<String> boundaries = consistentBoundaries(candidates, collationSorted);
    boundaries = verifyWithServer(source, boundaries);
    int dropped = candidates.size() - boundaries.size();
    if (dropped > 0) {
      LOG.warn(
          "{}: dropped {} of {} range boundaries whose byte order and AQL collation order"
              + " disagree; ranges are merged accordingly",
          collection,
          dropped,
          candidates.size());
    }
    List<KeyRange> ranges = toRanges(boundaries);
    LOG.info(
        "{}: sampled {} keys (p={}), planned {} ranges (requested {})",
        collection,
        samples.size(),
        probability,
        ranges.size(),
        n);
    return ranges;
  }

  /** Sorts by UTF-8 bytes and removes duplicates. */
  static List<String> byteSortedDistinct(Collection<String> keys) {
    TreeSet<String> set = new TreeSet<>(UTF8_BYTE_ORDER);
    for (String k : keys) {
      if (k != null) {
        set.add(k);
      }
    }
    return new ArrayList<>(set);
  }

  /**
   * Picks up to {@code n - 1} evenly spaced, distinct boundaries from a sorted distinct list. The
   * first element is never a boundary (it would produce an empty first range).
   */
  static List<String> quantiles(List<String> sortedDistinct, int n) {
    List<String> result = new ArrayList<>();
    int m = sortedDistinct.size();
    if (n <= 1 || m == 0) {
      return result;
    }
    int last = -1;
    for (int i = 1; i < n; i++) {
      int idx = (int) (((long) i * m) / n);
      if (idx <= 0 || idx >= m || idx == last) {
        continue;
      }
      result.add(sortedDistinct.get(idx));
      last = idx;
    }
    return result;
  }

  /**
   * Returns the longest subsequence of {@code byteSorted} whose positions in {@code
   * collationSorted} are strictly increasing, i.e. boundaries that increase in both orders.
   */
  static List<String> consistentBoundaries(List<String> byteSorted, List<String> collationSorted) {
    Map<String, Integer> rank = new HashMap<>();
    for (int i = 0; i < collationSorted.size(); i++) {
      rank.putIfAbsent(collationSorted.get(i), i);
    }
    int[] ranks = new int[byteSorted.size()];
    for (int i = 0; i < byteSorted.size(); i++) {
      Integer r = rank.get(byteSorted.get(i));
      if (r == null) {
        throw new IllegalStateException(
            "Server collation sort did not return boundary " + byteSorted.get(i));
      }
      ranks[i] = r;
    }
    int[] lis = longestStrictlyIncreasing(ranks);
    List<String> result = new ArrayList<>(lis.length);
    for (int idx : lis) {
      result.add(byteSorted.get(idx));
    }
    return result;
  }

  /**
   * Indices of a longest strictly increasing subsequence of {@code values} (O(n log n) patience
   * sorting with predecessor links).
   */
  static int[] longestStrictlyIncreasing(int[] values) {
    int n = values.length;
    int[] tailIdx = new int[n];
    int[] prev = new int[n];
    int length = 0;
    for (int i = 0; i < n; i++) {
      int lo = 0;
      int hi = length;
      while (lo < hi) {
        int mid = (lo + hi) >>> 1;
        if (values[tailIdx[mid]] < values[i]) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      prev[i] = lo > 0 ? tailIdx[lo - 1] : -1;
      tailIdx[lo] = i;
      if (lo == length) {
        length++;
      }
    }
    int[] result = new int[length];
    int k = length > 0 ? tailIdx[length - 1] : -1;
    for (int i = length - 1; i >= 0; i--) {
      result[i] = k;
      k = prev[k];
    }
    return result;
  }

  /**
   * Drops boundaries until the server agrees that each one is strictly less than the next. The
   * server's {@code <} is exactly the comparison the optimizer uses when folding range bounds.
   */
  static List<String> verifyWithServer(KeySource source, List<String> boundaries) {
    List<String> current = new ArrayList<>(boundaries);
    for (int attempt = 0; current.size() >= 2 && attempt <= boundaries.size(); attempt++) {
      List<Boolean> less = source.collationLessThanPairs(current);
      if (less.size() != current.size() - 1) {
        throw new IllegalStateException(
            "Expected " + (current.size() - 1) + " comparison results but got " + less.size());
      }
      List<String> next = new ArrayList<>(current.size());
      next.add(current.get(0));
      int lastKept = 0;
      boolean changed = false;
      for (int i = 1; i < current.size(); i++) {
        if (lastKept != i - 1) {
          // current[i-1] was dropped, so the pair (lastKept, i) is unverified: keep it for now
          // and re-verify in the next round.
          next.add(current.get(i));
          lastKept = i;
        } else if (Boolean.TRUE.equals(less.get(i - 1))) {
          next.add(current.get(i));
          lastKept = i;
        } else {
          changed = true;
        }
      }
      current = next;
      if (!changed) {
        return current;
      }
    }
    return current;
  }

  /** Turns strictly increasing boundaries into contiguous half-open ranges. */
  static List<KeyRange> toRanges(List<String> boundaries) {
    if (boundaries.isEmpty()) {
      return Collections.singletonList(KeyRange.ALL);
    }
    List<KeyRange> ranges = new ArrayList<>(boundaries.size() + 1);
    ranges.add(new KeyRange(null, boundaries.get(0)));
    for (int i = 0; i + 1 < boundaries.size(); i++) {
      ranges.add(new KeyRange(boundaries.get(i), boundaries.get(i + 1)));
    }
    ranges.add(new KeyRange(boundaries.get(boundaries.size() - 1), null));
    return ranges;
  }
}
