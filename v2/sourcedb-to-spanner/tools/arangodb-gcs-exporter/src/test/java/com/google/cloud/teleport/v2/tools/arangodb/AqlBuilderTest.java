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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AqlBuilderTest {

  private static final CollectionInfo USERS = new CollectionInfo("users", CollectionKind.DOCUMENT);
  private static final CollectionInfo KNOWS = new CollectionInfo("knows", CollectionKind.EDGE);

  @Test
  public void documentSinglePartitionHasNoFilter() {
    assertThat(AqlBuilder.partitionQuery(USERS, 1, 0))
        .isEqualTo(
            "FOR d IN `users` RETURN {key: d._key, id: d._id, rev: d._rev, doc:"
                + " JSON_STRINGIFY(d)}");
  }

  @Test
  public void edgeHashPartition() {
    assertThat(AqlBuilder.partitionQuery(KNOWS, 8, 3))
        .isEqualTo(
            "FOR d IN `knows` FILTER HASH(d._key) % 8 == 3 RETURN {key: d._key, id: d._id, rev:"
                + " d._rev, from_id: d._from, to_id: d._to, doc: JSON_STRINGIFY(d)}");
  }

  @Test
  public void invalidPartitionThrows() {
    assertThrows(IllegalArgumentException.class, () -> AqlBuilder.partitionQuery(USERS, 8, 8));
    assertThrows(IllegalArgumentException.class, () -> AqlBuilder.partitionQuery(USERS, 0, 0));
  }

  @Test
  public void rangeQueries() {
    String ret = "RETURN {key: d._key, id: d._id, rev: d._rev, doc: JSON_STRINGIFY(d)}";
    assertThat(AqlBuilder.rangeQuery(USERS, KeyRange.ALL)).isEqualTo("FOR d IN `users` " + ret);
    assertThat(AqlBuilder.rangeQuery(USERS, new KeyRange(null, "m")))
        .isEqualTo("FOR d IN `users` FILTER d._key < \"m\" " + ret);
    assertThat(AqlBuilder.rangeQuery(USERS, new KeyRange("m", null)))
        .isEqualTo("FOR d IN `users` FILTER d._key >= \"m\" " + ret);
    assertThat(AqlBuilder.rangeQuery(USERS, new KeyRange("a", "b")))
        .isEqualTo("FOR d IN `users` FILTER d._key >= \"a\" AND d._key < \"b\" " + ret);
  }

  @Test
  public void hostileBoundariesAreEscaped() throws Exception {
    String[] hostile = {
      "a\"b", "back\\slash", "it's", "%41", "(x)+y,z=1;$!*", "a:b.c@d", "new\nline", "\u0001ctl"
    };
    ObjectMapper mapper = new ObjectMapper();
    for (String key : hostile) {
      String literal = AqlBuilder.aqlString(key);
      assertThat(literal).startsWith("\"");
      assertThat(literal).endsWith("\"");
      // AQL double-quoted strings use JSON escapes; the literal must decode back to the key.
      assertThat(mapper.readValue(literal, String.class)).isEqualTo(key);
      String q = AqlBuilder.rangeQuery(USERS, new KeyRange(key, null));
      assertThat(q).contains("d._key >= " + literal + " RETURN");
    }
  }

  @Test
  public void badCollectionNamesThrow() {
    assertThrows(IllegalArgumentException.class, () -> AqlBuilder.quoteCollection("a`b"));
    assertThrows(IllegalArgumentException.class, () -> AqlBuilder.quoteCollection("a b"));
    assertThrows(IllegalArgumentException.class, () -> AqlBuilder.quoteCollection(""));
    assertThat(AqlBuilder.quoteCollection("user-profiles_1")).isEqualTo("`user-profiles_1`");
    assertThrows(
        IllegalArgumentException.class,
        () -> AqlBuilder.partitionQuery(new CollectionInfo("x y", CollectionKind.DOCUMENT), 1, 0));
  }

  @Test
  public void countQueryUsesSameFilter() {
    String filter = AqlBuilder.rangeFilter(new KeyRange("a", "b"));
    assertThat(AqlBuilder.exportQuery(USERS, filter))
        .isEqualTo(AqlBuilder.rangeQuery(USERS, new KeyRange("a", "b")));
    assertThat(AqlBuilder.countQuery(USERS, filter))
        .isEqualTo(
            "FOR d IN `users` FILTER d._key >= \"a\" AND d._key < \"b\" COLLECT WITH COUNT INTO n"
                + " RETURN n");
    assertThat(AqlBuilder.countQuery(KNOWS, AqlBuilder.hashFilter(KNOWS, 2, 1)))
        .isEqualTo(
            "FOR d IN `knows` FILTER HASH(d._key) % 2 == 1 COLLECT WITH COUNT INTO n RETURN n");
    assertThat(AqlBuilder.countQuery(USERS, ""))
        .isEqualTo("FOR d IN `users` COLLECT WITH COUNT INTO n RETURN n");
  }
}
