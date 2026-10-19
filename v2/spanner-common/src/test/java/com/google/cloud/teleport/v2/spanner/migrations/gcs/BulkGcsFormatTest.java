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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BulkGcsFormat}. */
@RunWith(JUnit4.class)
public class BulkGcsFormatTest {

  private static final String ROOT = "gs://my-bucket/dir";

  @Test
  public void filePatternsWithoutFilters() {
    assertEquals(List.of(ROOT + "/**.avro"), BulkGcsFormat.filePatterns(ROOT, null, null));
    assertEquals(
        List.of(ROOT + "/**.avro"), BulkGcsFormat.filePatterns(ROOT + "/", Set.of(), Set.of()));
    assertFalse(BulkGcsFormat.needsFileFilter(null, null));
    assertFalse(BulkGcsFormat.needsFileFilter(Set.of(), Set.of()));
  }

  @Test
  public void filePatternsWithTables() {
    List<String> patterns = BulkGcsFormat.filePatterns(ROOT, Set.of("Table1", "Table2"), null);
    assertEquals(
        Set.of(ROOT + "/Table1/**.avro", ROOT + "/Table2/**.avro"), new HashSet<>(patterns));
    assertEquals(2, patterns.size());
    assertFalse(BulkGcsFormat.needsFileFilter(Set.of("Table1"), null));
  }

  @Test
  public void filePatternsWithTablesAndShards() {
    List<String> patterns =
        BulkGcsFormat.filePatterns(ROOT, Set.of("T1", "T2"), Set.of("s1", "s2"));
    assertEquals(
        Set.of(
            ROOT + "/T1/s1/**.avro",
            ROOT + "/T1/s2/**.avro",
            ROOT + "/T2/s1/**.avro",
            ROOT + "/T2/s2/**.avro"),
        new HashSet<>(patterns));
    assertEquals(4, patterns.size());
    assertFalse(BulkGcsFormat.needsFileFilter(Set.of("T1"), Set.of("s1")));
  }

  @Test
  public void filePatternsWithShardsOnlyMatchesEverythingOnce() {
    assertEquals(
        List.of(ROOT + "/**.avro"), BulkGcsFormat.filePatterns(ROOT, null, Set.of("s1", "s2")));
    assertTrue(BulkGcsFormat.needsFileFilter(null, Set.of("s1")));
  }

  @Test
  public void filePatternsWithGlobCharactersMatchesEverythingOnce() {
    for (String name : List.of("a*b", "a?b", "a[b", "a\\b")) {
      assertEquals(
          List.of(ROOT + "/**.avro"),
          BulkGcsFormat.filePatterns(ROOT, Set.of(name, "users"), Set.of("s1")));
      assertTrue(BulkGcsFormat.needsFileFilter(Set.of(name), null));
      assertTrue(BulkGcsFormat.needsFileFilter(Set.of("users"), Set.of(name)));
    }
  }

  @Test
  public void isConfiguredFileMatchesExactTableAndShard() {
    Set<String> tables = Set.of("a*b", "users");
    Set<String> shards = Set.of("s[1");
    assertTrue(BulkGcsFormat.isConfiguredFile(ROOT, tables, shards, ROOT + "/a*b/s[1/f.avro"));
    assertTrue(BulkGcsFormat.isConfiguredFile(ROOT, tables, shards, ROOT + "/users/s[1/f.avro"));
    assertFalse(BulkGcsFormat.isConfiguredFile(ROOT, tables, shards, ROOT + "/axb/s[1/f.avro"));
    assertFalse(BulkGcsFormat.isConfiguredFile(ROOT, tables, shards, ROOT + "/a*b/sx1/f.avro"));
    assertFalse(BulkGcsFormat.isConfiguredFile(ROOT, tables, shards, ROOT + "/a*b/f.avro"));
    // Trailing slash on root is ignored.
    assertTrue(
        BulkGcsFormat.isConfiguredFile(ROOT + "/", tables, shards, ROOT + "/a*b/s[1/f.avro"));
  }

  @Test
  public void isConfiguredFileWithoutShardFilter() {
    assertTrue(BulkGcsFormat.isConfiguredFile(ROOT, Set.of("t"), null, ROOT + "/t/f.avro"));
    assertTrue(BulkGcsFormat.isConfiguredFile(ROOT, Set.of("t"), null, ROOT + "/t/s/f.avro"));
    assertFalse(BulkGcsFormat.isConfiguredFile(ROOT, Set.of("t"), null, ROOT + "/u/f.avro"));
    assertTrue(BulkGcsFormat.isConfiguredFile(ROOT, null, Set.of("s"), ROOT + "/u/s/f.avro"));
    assertFalse(BulkGcsFormat.isConfiguredFile(ROOT, null, Set.of("s"), ROOT + "/u/s10/f.avro"));
  }

  @Test
  public void stripTrailingSlash() {
    assertEquals("gs://b/d", BulkGcsFormat.stripTrailingSlash("gs://b/d/"));
    assertEquals("gs://b/d", BulkGcsFormat.stripTrailingSlash("gs://b/d"));
  }
}
