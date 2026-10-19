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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Produces a canonical text for a JSON document so that semantically equal documents hash equally.
 *
 * <p>Spanner stores JSON in a normalized form: object keys are reordered, whitespace is dropped and
 * numbers may be re-printed (for example {@code -9223372036854775808.0} becomes {@code
 * -9.223372036854776e+18}). Source records (for example bulk migration Avro files) carry the
 * source's original text, so hashing the raw text makes every JSON value a false mismatch. The
 * canonical form:
 *
 * <ul>
 *   <li>sorts object keys (recursively) and writes no insignificant whitespace;
 *   <li>re-escapes strings consistently;
 *   <li>writes a number by value: an integral value that fits in a {@code long} as that integer,
 *       anything else as the shortest round-trip text of its {@code double} value (the precision
 *       Spanner JSON stores).
 * </ul>
 *
 * Text that is not valid JSON is returned unchanged.
 */
public final class JsonCanonicalizer {

  private static final JsonFactory FACTORY =
      JsonFactory.builder()
          .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
          .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
          .build();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

  private JsonCanonicalizer() {}

  /** Returns the canonical text of {@code json}, or {@code json} itself if it is not valid JSON. */
  public static String canonicalize(String json) {
    if (json == null) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      if (root == null || root.isMissingNode()) {
        return json;
      }
      StringWriter out = new StringWriter(json.length());
      try (JsonGenerator gen = FACTORY.createGenerator(out)) {
        write(root, gen);
      }
      return out.toString();
    } catch (IOException e) {
      return json;
    }
  }

  private static void write(JsonNode node, JsonGenerator gen) throws IOException {
    if (node.isObject()) {
      List<String> names = new ArrayList<>();
      for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
        names.add(it.next());
      }
      Collections.sort(names);
      gen.writeStartObject();
      for (String name : names) {
        gen.writeFieldName(name);
        write(node.get(name), gen);
      }
      gen.writeEndObject();
    } else if (node.isArray()) {
      gen.writeStartArray();
      for (JsonNode element : node) {
        write(element, gen);
      }
      gen.writeEndArray();
    } else if (node.isNumber()) {
      writeNumber(node, gen);
    } else if (node.isTextual()) {
      gen.writeString(node.textValue());
    } else if (node.isBoolean()) {
      gen.writeBoolean(node.booleanValue());
    } else {
      gen.writeNull();
    }
  }

  private static void writeNumber(JsonNode node, JsonGenerator gen) throws IOException {
    if (node.isIntegralNumber()) {
      BigInteger value = node.bigIntegerValue();
      if (value.bitLength() < 64) {
        gen.writeNumber(value.longValue());
      } else {
        writeDouble(value.doubleValue(), gen);
      }
      return;
    }
    writeDouble(node.doubleValue(), gen);
  }

  private static void writeDouble(double d, JsonGenerator gen) throws IOException {
    if (!Double.isNaN(d) && !Double.isInfinite(d) && d == Math.rint(d)) {
      BigDecimal exact = new BigDecimal(d);
      if (exact.compareTo(LONG_MIN) >= 0 && exact.compareTo(LONG_MAX) <= 0) {
        gen.writeNumber(exact.longValueExact());
        return;
      }
    }
    gen.writeNumber(d);
  }
}
