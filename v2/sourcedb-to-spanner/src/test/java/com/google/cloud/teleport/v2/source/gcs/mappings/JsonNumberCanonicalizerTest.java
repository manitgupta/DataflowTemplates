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
package com.google.cloud.teleport.v2.source.gcs.mappings;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonNumberCanonicalizerTest {

  @Test
  public void rewritesNonShortestDoublesSeenInArangoDbExports() {
    assertThat(JsonNumberCanonicalizer.canonicalize("{\"weight\":0.46245000000000003}"))
        .isEqualTo("{\"weight\":0.46245}");
    assertThat(JsonNumberCanonicalizer.canonicalize("{\"big\":-9223372036854775808.0}"))
        .isEqualTo("{\"big\":-9.223372036854776E18}");
  }

  @Test
  public void rewrittenDoublesAreTheSameValue() {
    for (String s :
        new String[] {
          "0.46245000000000003",
          "0.1",
          "1e+300",
          "5e-324",
          "1.7976931348623157e+308",
          "-0.0",
          "123.456",
          "2.5E-7"
        }) {
      String out = JsonNumberCanonicalizer.canonicalize("[" + s + "]");
      double back = Double.parseDouble(out.substring(1, out.length() - 1));
      assertThat(Double.doubleToRawLongBits(back))
          .isEqualTo(Double.doubleToRawLongBits(Double.parseDouble(s)));
    }
  }

  @Test
  public void keepsIntegersStringsAndStructureUnchanged() {
    String json =
        "{\"_key\":\"k.1\",\"v\":9007199254740993,\"u\":18446744073709551615,"
            + "\"n\":-9223372036854775808,\"s\":\"1.50 e \\\" \\\\ é 🚀\",\"a\":[1,\"x\",null,true,{}],"
            + "\"o\":{\"deep\":{\"x\":null}},\"attr with space\":\"x\"}";
    assertThat(JsonNumberCanonicalizer.canonicalize(json)).isEqualTo(json);
  }

  @Test
  public void keepsTextWhenItIsAlreadyAsShortAsJavaShortestForm() {
    // Java's shortest form of the minimum subnormal is 4.9E-324 (two digits); Spanner only accepts
    // 5e-324, which is what sources such as ArangoDB already write.
    assertThat(JsonNumberCanonicalizer.canonicalize("[5e-324,1e+300,0.1,1.5,-0.0]"))
        .isEqualTo("[5e-324,1e+300,0.1,1.5,-0.0]");
  }

  @Test
  public void significantDigits() {
    assertThat(JsonNumberCanonicalizer.significantDigits("0.46245000000000003", false))
        .isEqualTo(17);
    assertThat(JsonNumberCanonicalizer.significantDigits("0.46245", true)).isEqualTo(5);
    assertThat(JsonNumberCanonicalizer.significantDigits("-9223372036854775808.0", false))
        .isEqualTo(20);
    assertThat(JsonNumberCanonicalizer.significantDigits("-9.223372036854776E18", true))
        .isEqualTo(16);
    assertThat(JsonNumberCanonicalizer.significantDigits("5e-324", false)).isEqualTo(1);
    assertThat(JsonNumberCanonicalizer.significantDigits("4.9E-324", true)).isEqualTo(2);
    assertThat(JsonNumberCanonicalizer.significantDigits("0.0", false)).isEqualTo(1);
    assertThat(JsonNumberCanonicalizer.significantDigits("1.50", false)).isEqualTo(3);
    assertThat(JsonNumberCanonicalizer.significantDigits("1.50", true)).isEqualTo(2);
    assertThat(JsonNumberCanonicalizer.significantDigits("100.0", true)).isEqualTo(1);
  }

  @Test
  public void stripsSourceTrailingZeros() {
    assertThat(JsonNumberCanonicalizer.canonicalize("{\"n\":1.50,\"m\":0.0258260}"))
        .isEqualTo("{\"n\":1.5,\"m\":0.025826}");
  }

  @Test
  public void invalidJsonIsReturnedAsIs() {
    assertThat(JsonNumberCanonicalizer.canonicalize("{\"a\":1.50")).isEqualTo("{\"a\":1.50");
    assertThat(JsonNumberCanonicalizer.canonicalize("not json.")).isEqualTo("not json.");
  }

  @Test
  public void textWithoutFloatMarkersIsReturnedAsIs() {
    String json = "{\"a\":1,\"b\":[2,3]}";
    assertThat(JsonNumberCanonicalizer.canonicalize(json)).isSameInstanceAs(json);
  }
}
