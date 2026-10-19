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
package com.google.cloud.teleport.v2.visitor;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonCanonicalizerTest {

  @Test
  public void sourceTextAndSpannerNormalizedTextCanonicalizeEqually() {
    // ArangoDB JSON_STRINGIFY text vs. what Spanner returns for the same document.
    String source =
        "{\"_key\":\"k1\",\"_id\":\"users/k1\",\"w\":0.46245000000000003,"
            + "\"big\":-9223372036854775808.0,\"u\":18446744073709551615,\"n\":{\"b\":[1,2.50,{\"z\":1,\"a\":null}],\"a\":true},"
            + "\"s\":\"caf\\u00e9 \\\"q\\\"\"}";
    String spanner =
        "{\"_id\":\"users/k1\",\"_key\":\"k1\",\"big\":-9.223372036854776e+18,"
            + "\"n\":{\"a\":true,\"b\":[1,2.5,{\"a\":null,\"z\":1}]},\"s\":\"café \\\"q\\\"\","
            + "\"u\":1.8446744073709552e+19,\"w\":0.46245}";
    assertThat(JsonCanonicalizer.canonicalize(source))
        .isEqualTo(JsonCanonicalizer.canonicalize(spanner));
  }

  @Test
  public void differentValuesStayDifferent() {
    // 0.6010490000000001 is the same double as 0.601049 (Spanner prints it that way); the next
    // double up is 0.6010490000000002.
    assertThat(JsonCanonicalizer.canonicalize("{\"x\":0.601049}"))
        .isEqualTo(JsonCanonicalizer.canonicalize("{\"x\":0.6010490000000001}"));
    assertThat(JsonCanonicalizer.canonicalize("{\"x\":0.601049}"))
        .isNotEqualTo(JsonCanonicalizer.canonicalize("{\"x\":0.6010490000000002}"));
    assertThat(JsonCanonicalizer.canonicalize("{\"x\":\"1\"}"))
        .isNotEqualTo(JsonCanonicalizer.canonicalize("{\"x\":1}"));
    assertThat(JsonCanonicalizer.canonicalize("[1,2]"))
        .isNotEqualTo(JsonCanonicalizer.canonicalize("[2,1]"));
  }

  @Test
  public void numbersAreWrittenByValue() {
    assertThat(JsonCanonicalizer.canonicalize("[100.0,1e2,-0.0,9007199254740993,1e300,5e-324]"))
        .isEqualTo("[100,100,0,9007199254740993,1.0E300,4.9E-324]");
  }

  @Test
  public void invalidJsonIsReturnedUnchanged() {
    assertThat(JsonCanonicalizer.canonicalize("{\"a\":")).isEqualTo("{\"a\":");
    assertThat(JsonCanonicalizer.canonicalize("")).isEqualTo("");
    assertThat(JsonCanonicalizer.canonicalize(null)).isNull();
  }
}
