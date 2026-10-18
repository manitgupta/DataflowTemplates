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

import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericFixed;

/**
 * Maps the manifest column types of a GCS file export to unified types, and converts Avro values
 * read from the export files to the payload values expected by the unified Avro schema.
 */
public final class GcsMappingsProvider {

  private static final ImmutableMap<String, UnifiedMappingProvider.Type> TYPES =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("STRING", UnifiedMappingProvider.Type.STRING)
          .put("JSON", UnifiedMappingProvider.Type.JSON)
          .put("LONG", UnifiedMappingProvider.Type.LONG)
          .put("DOUBLE", UnifiedMappingProvider.Type.DOUBLE)
          .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          .put("BYTES", UnifiedMappingProvider.Type.BYTES)
          .build();

  /** Manifest column types supported by the GCS source (upper case). */
  public static final ImmutableSet<String> SUPPORTED_TYPES = TYPES.keySet();

  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      TYPES.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  e -> e.getKey(), e -> UnifiedMappingProvider.getMapping(e.getValue())));

  private GcsMappingsProvider() {}

  /** Returns the mapping from manifest type name to unified type mapping. */
  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return MAPPING;
  }

  /** Returns a {@link UnifiedTypeMapper} for manifest column types. */
  public static UnifiedTypeMapper typeMapper() {
    return new UnifiedTypeMapper(getMapping());
  }

  /**
   * Converts a value read from an export Avro file to the payload value for the given manifest
   * type. {@code null} stays {@code null}. STRING and JSON never fail.
   *
   * @param type manifest column type (case-insensitive).
   * @param avroValue value read from the Avro file.
   * @return the payload value.
   * @throws IllegalArgumentException if the value cannot be converted to the type.
   */
  @Nullable
  public static Object toPayloadValue(String type, @Nullable Object avroValue) {
    if (avroValue == null) {
      return null;
    }
    String upperType = type == null ? "" : type.toUpperCase(Locale.ROOT);
    try {
      switch (upperType) {
        case "STRING":
          return toStringValue(avroValue);
        case "JSON":
          return JsonNumberCanonicalizer.canonicalize(toStringValue(avroValue));
        case "LONG":
          return toLong(avroValue, upperType);
        case "DOUBLE":
          return toDouble(avroValue, upperType);
        case "BOOLEAN":
          return toBoolean(avroValue, upperType);
        case "TIMESTAMP":
          return toTimestampMicros(avroValue, upperType);
        case "DATE":
          return toEpochDay(avroValue, upperType);
        case "BYTES":
          return toBytes(avroValue, upperType);
        default:
          throw new IllegalArgumentException("Unsupported GCS source column type " + type);
      }
    } catch (NumberFormatException | DateTimeException | ArithmeticException e) {
      throw unconvertible(upperType, avroValue, e);
    }
  }

  private static String toStringValue(Object value) {
    if (value instanceof ByteBuffer) {
      return StandardCharsets.UTF_8.decode(((ByteBuffer) value).duplicate()).toString();
    }
    return value.toString();
  }

  private static Long toLong(Object value, String type) {
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return ((Number) value).longValue();
    }
    if (value instanceof CharSequence) {
      return Long.parseLong(value.toString().trim());
    }
    throw unconvertible(type, value, null);
  }

  private static Double toDouble(Object value, String type) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof CharSequence) {
      return Double.parseDouble(value.toString().trim());
    }
    throw unconvertible(type, value, null);
  }

  private static Boolean toBoolean(Object value, String type) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof CharSequence) {
      String s = value.toString().trim();
      if ("true".equalsIgnoreCase(s)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(s)) {
        return Boolean.FALSE;
      }
    }
    throw unconvertible(type, value, null);
  }

  private static Long toTimestampMicros(Object value, String type) {
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof CharSequence) {
      Instant instant = Instant.parse(value.toString().trim());
      return Math.addExact(
          Math.multiplyExact(instant.getEpochSecond(), TimeUnit.SECONDS.toMicros(1)),
          TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
    }
    throw unconvertible(type, value, null);
  }

  private static Integer toEpochDay(Object value, String type) {
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value instanceof CharSequence) {
      return Math.toIntExact(LocalDate.parse(value.toString().trim()).toEpochDay());
    }
    throw unconvertible(type, value, null);
  }

  private static ByteBuffer toBytes(Object value, String type) {
    if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    }
    if (value instanceof GenericFixed) {
      return ByteBuffer.wrap(((GenericFixed) value).bytes());
    }
    throw unconvertible(type, value, null);
  }

  private static IllegalArgumentException unconvertible(
      String type, Object value, @Nullable Throwable cause) {
    String shown = String.valueOf(value);
    if (shown.length() > 100) {
      shown = shown.substring(0, 100) + "...";
    }
    return new IllegalArgumentException(
        "Cannot convert value '"
            + shown
            + "' of class "
            + value.getClass().getName()
            + " to "
            + type,
        cause);
  }
}
