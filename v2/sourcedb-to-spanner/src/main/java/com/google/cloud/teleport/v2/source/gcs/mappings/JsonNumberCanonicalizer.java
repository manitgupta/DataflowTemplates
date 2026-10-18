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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Rewrites the floating-point numbers of a JSON text in their shortest round-trip form.
 *
 * <p>Spanner rejects a JSON number whose text is not the shortest representation of the double it
 * parses to ("cannot round-trip through string representation"). Some sources print doubles with 17
 * significant digits or a trailing {@code .0}: ArangoDB's {@code JSON_STRINGIFY} writes {@code
 * 0.46245000000000003} for the double whose shortest form is {@code 0.46245}, and {@code
 * -9223372036854775808.0} for {@code -9.223372036854776E18}. Both texts denote exactly the same
 * double as their shortest form, so rewriting them is lossless. Integers, strings, key order and
 * everything else are copied unchanged. Text that is not valid JSON is returned as is (Spanner then
 * rejects the row, which goes to the dead letter queue as before).
 */
public final class JsonNumberCanonicalizer {

  // Schubfach-based writer: shortest round-trip output (JDK 17's Double.toString is not).
  private static final JsonFactory FACTORY =
      JsonFactory.builder()
          .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
          .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
          .build();

  private JsonNumberCanonicalizer() {}

  /** Returns {@code json} with every floating-point number in shortest round-trip form. */
  public static String canonicalize(String json) {
    if (!mayContainFloat(json)) {
      return json;
    }
    StringWriter out = new StringWriter(json.length());
    try (JsonParser parser = FACTORY.createParser(json);
        JsonGenerator gen = FACTORY.createGenerator(out)) {
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == JsonToken.VALUE_NUMBER_FLOAT) {
          String original = parser.getText();
          String shortest = SHORTEST.get().apply(parser.getDoubleValue());
          // Java's shortest form keeps at least two digits (4.9E-324 for 5e-324), so only rewrite
          // when it has fewer digits than the source wrote. The source's trailing zeros count
          // (Spanner also rejects 1.50), Java's own trailing ".0" does not (Spanner accepts 2.0).
          gen.writeNumber(
              significantDigits(shortest, true) < significantDigits(original, false)
                  ? shortest
                  : original);
        } else {
          gen.copyCurrentEvent(parser);
        }
      }
    } catch (IOException e) {
      return json;
    }
    return out.toString();
  }

  /** Shortest round-trip text of a double (Schubfach, via Jackson's fast double writer). */
  private static final ThreadLocal<java.util.function.DoubleFunction<String>> SHORTEST =
      ThreadLocal.withInitial(
          () ->
              d -> {
                StringWriter w = new StringWriter(24);
                try (JsonGenerator g = FACTORY.createGenerator(w)) {
                  g.writeNumber(d);
                } catch (IOException e) {
                  return Double.toString(d);
                }
                return w.toString();
              });

  /**
   * Number of mantissa digits of a JSON number text, leading zeros and the exponent excluded.
   *
   * @param stripTrailingZeros whether trailing zeros of the mantissa are excluded too.
   */
  static int significantDigits(String number, boolean stripTrailingZeros) {
    int end = number.length();
    for (int i = 0; i < number.length(); i++) {
      char c = number.charAt(i);
      if (c == 'e' || c == 'E') {
        end = i;
        break;
      }
    }
    String mantissa = number.substring(0, end).replace("-", "").replace(".", "");
    int first = 0;
    while (first < mantissa.length() - 1 && mantissa.charAt(first) == '0') {
      first++;
    }
    int last = mantissa.length();
    while (stripTrailingZeros && last > first + 1 && mantissa.charAt(last - 1) == '0') {
      last--;
    }
    return last - first;
  }

  /** Cheap pre-check: a JSON float needs a '.', 'e' or 'E' somewhere in the text. */
  private static boolean mayContainFloat(String json) {
    return json.indexOf('.') >= 0 || json.indexOf('e') >= 0 || json.indexOf('E') >= 0;
  }
}
