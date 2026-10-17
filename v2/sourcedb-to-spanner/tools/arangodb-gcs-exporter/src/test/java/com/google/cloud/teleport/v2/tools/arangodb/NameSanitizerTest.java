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
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NameSanitizerTest {

  @Test
  public void sanitizesCharacters() {
    assertThat(NameSanitizer.toTableName("user-profiles")).isEqualTo("user_profiles");
    assertThat(NameSanitizer.toTableName("a.b")).isEqualTo("a_b");
    assertThat(NameSanitizer.toTableName("Users")).isEqualTo("users");
    assertThat(NameSanitizer.toTableName("knows")).isEqualTo("knows");
  }

  @Test
  public void prefixesNonLetterStart() {
    assertThat(NameSanitizer.toTableName("1abc")).isEqualTo("t_1abc");
    assertThat(NameSanitizer.toTableName("_x")).isEqualTo("t__x");
    assertThat(NameSanitizer.toTableName("-x")).isEqualTo("t__x");
  }

  @Test
  public void truncatesTo128() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append('a');
    }
    String table = NameSanitizer.toTableName(sb.toString());
    assertThat(table).hasLength(128);
  }

  @Test
  public void collisionThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> NameSanitizer.toTableNames(Arrays.asList("Users", "users")));
    assertThrows(
        IllegalArgumentException.class,
        () -> NameSanitizer.toTableNames(Arrays.asList("a-b", "a_b")));
  }

  @Test
  public void mapsInOrder() {
    Map<String, String> m = NameSanitizer.toTableNames(Arrays.asList("b-1", "a"));
    assertThat(m).containsExactly("b-1", "b_1", "a", "a").inOrder();
  }

  @Test
  public void emptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> NameSanitizer.toTableName(""));
  }
}
