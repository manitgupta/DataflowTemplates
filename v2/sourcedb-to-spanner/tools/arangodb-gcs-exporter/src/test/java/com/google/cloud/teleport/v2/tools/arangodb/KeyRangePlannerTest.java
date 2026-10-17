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

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KeyRangePlannerTest {

  /** Every character class allowed in a traditional ArangoDB _key. */
  private static final String KEY_ALPHABET =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-:.@()+,=;$!*'%";

  static List<String> hostileKeys(int count, long seed) {
    Random r = new Random(seed);
    Set<String> keys = new LinkedHashSet<>();
    // Fixed fixtures known to disagree between byte order and collation.
    keys.addAll(
        Arrays.asList(
            "a", "B", "Z", "_x", "A", "b", "z", "-", "_", "0", "9", "%41", "'q'", "(x)", "a:b",
            "a.b", "a@b", "x+y", "k=v", "a;b", "$1", "!", "*", "AbC", "aBc", "abc", "ABC"));
    while (keys.size() < count) {
      int len = 1 + r.nextInt(12);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < len; i++) {
        sb.append(KEY_ALPHABET.charAt(r.nextInt(KEY_ALPHABET.length())));
      }
      keys.add(sb.toString());
    }
    return new ArrayList<>(keys);
  }

  private static void assertDisjointAndComplete(
      FakeArangoKeySource server, List<String> keys, List<KeyRange> ranges) {
    long sum = 0;
    for (KeyRange r : ranges) {
      sum += server.countRange(r);
    }
    assertThat(sum).isEqualTo(keys.size());
    for (String k : keys) {
      int hits = 0;
      for (KeyRange r : ranges) {
        if (FakeArangoKeySource.inByteRange(k, r)) {
          hits++;
        }
      }
      assertThat(hits).isEqualTo(1);
    }
  }

  @Test
  public void hostileKeysRangesAreDisjointAndComplete() {
    for (long seed = 1; seed <= 5; seed++) {
      List<String> keys = hostileKeys(20_000, seed);
      FakeArangoKeySource server = new FakeArangoKeySource(keys, seed);
      List<KeyRange> ranges = KeyRangePlanner.plan(server, "hostile", keys.size(), 50);
      assertThat(ranges.size()).isGreaterThan(1);
      assertThat(ranges.size()).isAtMost(50);
      assertThat(ranges.get(0).lo()).isNull();
      assertThat(ranges.get(ranges.size() - 1).hi()).isNull();
      for (int i = 0; i + 1 < ranges.size(); i++) {
        assertThat(ranges.get(i).hi()).isEqualTo(ranges.get(i + 1).lo());
        String lo = ranges.get(i + 1).lo();
        String hi = ranges.get(i + 1).hi();
        if (lo != null && hi != null) {
          assertThat(KeyRangePlanner.UTF8_BYTE_ORDER.compare(lo, hi)).isLessThan(0);
          assertThat(FakeArangoKeySource.COLLATION.compare(lo, hi)).isLessThan(0);
        }
      }
      assertDisjointAndComplete(server, keys, ranges);
    }
  }

  @Test
  public void naiveByteQuantilesWouldLoseRows() {
    // Shows that the simulated hazard is real: without the collation step some ranges fold to
    // EMPTY on the server and rows are silently lost.
    List<String> keys = hostileKeys(20_000, 7);
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 7);
    List<String> naive = KeyRangePlanner.quantiles(KeyRangePlanner.byteSortedDistinct(keys), 50);
    long sum = 0;
    for (KeyRange r : KeyRangePlanner.toRanges(naive)) {
      sum += server.countRange(r);
    }
    assertThat(sum).isLessThan((long) keys.size());
  }

  @Test
  public void uniformLowercaseKeysKeepAllBoundaries() {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      keys.add(String.format("k%08d", i));
    }
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 1);
    List<KeyRange> ranges = KeyRangePlanner.plan(server, "c", keys.size(), 10);
    assertThat(ranges).hasSize(10);
    assertDisjointAndComplete(server, keys, ranges);
  }

  @Test
  public void samplingProbabilityIsBounded() {
    List<String> keys = hostileKeys(5_000, 3);
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 3);
    // count is claimed to be huge, so p is tiny and only few samples come back.
    List<KeyRange> ranges = KeyRangePlanner.plan(server, "c", 1_000_000_000L, 4);
    assertThat(server.sampleCalls).isEqualTo(1);
    assertDisjointAndComplete(server, keys, ranges);
  }

  @Test
  public void singlePartitionOrEmptyIsAll() {
    FakeArangoKeySource server = new FakeArangoKeySource(Collections.emptyList(), 1);
    assertThat(KeyRangePlanner.plan(server, "c", 100, 1)).containsExactly(KeyRange.ALL);
    assertThat(KeyRangePlanner.plan(server, "c", 0, 8)).containsExactly(KeyRange.ALL);
    // Count says 100 but sampling returns nothing (e.g. deleted meanwhile).
    assertThat(KeyRangePlanner.plan(server, "c", 100, 8)).containsExactly(KeyRange.ALL);
    assertThat(server.sortCalls).isEqualTo(0);
  }

  @Test
  public void byteOrderIsUnsignedUtf8() {
    List<String> sorted =
        KeyRangePlanner.byteSortedDistinct(Arrays.asList("b", "a", "B", "_", "a", "é", "z"));
    assertThat(sorted).containsExactly("B", "_", "a", "b", "z", "é").inOrder();
  }

  @Test
  public void quantilesAreDistinctAndSkipFirst() {
    List<String> s = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");
    assertThat(KeyRangePlanner.quantiles(s, 4)).containsExactly("c", "e", "g").inOrder();
    assertThat(KeyRangePlanner.quantiles(s, 1)).isEmpty();
    assertThat(KeyRangePlanner.quantiles(Arrays.asList("a", "b"), 10)).containsExactly("b");
    assertThat(KeyRangePlanner.quantiles(Collections.emptyList(), 10)).isEmpty();
  }

  @Test
  public void longestStrictlyIncreasing() {
    assertThat(KeyRangePlanner.longestStrictlyIncreasing(new int[] {}).length).isEqualTo(0);
    int[] values = {3, 1, 4, 1, 5, 9, 2, 6};
    int[] idx = KeyRangePlanner.longestStrictlyIncreasing(values);
    assertThat(idx.length).isEqualTo(4); // e.g. 1,4,5,9 or 3,4,5,6
    for (int i = 0; i + 1 < idx.length; i++) {
      assertThat(idx[i]).isLessThan(idx[i + 1]);
      assertThat(values[idx[i]]).isLessThan(values[idx[i + 1]]);
    }
    assertThat(KeyRangePlanner.longestStrictlyIncreasing(new int[] {5, 5, 5}).length).isEqualTo(1);
  }

  @Test
  public void consistentBoundariesDropCollationInversions() {
    // Byte order: B < Z < _x < a; collation (fake): _x < a < B < Z.
    List<String> byteSorted = Arrays.asList("B", "Z", "_x", "a");
    FakeArangoKeySource server = new FakeArangoKeySource(Collections.emptyList(), 1);
    List<String> kept =
        KeyRangePlanner.consistentBoundaries(byteSorted, server.collationSort(byteSorted));
    assertThat(kept).hasSize(2);
    for (int i = 0; i + 1 < kept.size(); i++) {
      assertThat(KeyRangePlanner.UTF8_BYTE_ORDER.compare(kept.get(i), kept.get(i + 1)))
          .isLessThan(0);
      assertThat(FakeArangoKeySource.COLLATION.compare(kept.get(i), kept.get(i + 1))).isLessThan(0);
    }
  }

  @Test
  public void serverVerificationDropsDisagreeingBoundary() {
    FakeArangoKeySource server = new FakeArangoKeySource(Collections.emptyList(), 1);
    server.disagreeOn("b", "c");
    List<String> kept = KeyRangePlanner.verifyWithServer(server, Arrays.asList("a", "b", "c", "d"));
    assertThat(kept).containsExactly("a", "b", "d").inOrder();
    assertThat(server.lessCalls).isEqualTo(2);
  }

  @Test
  public void toRanges() {
    assertThat(KeyRangePlanner.toRanges(Collections.emptyList())).containsExactly(KeyRange.ALL);
    assertThat(KeyRangePlanner.toRanges(Arrays.asList("m", "t")))
        .containsExactly(new KeyRange(null, "m"), new KeyRange("m", "t"), new KeyRange("t", null))
        .inOrder();
    assertThat(KeyRange.ALL.isAll()).isTrue();
    assertThat(new KeyRange("a", null).toString()).isEqualTo("[a, +inf)");
  }
}
