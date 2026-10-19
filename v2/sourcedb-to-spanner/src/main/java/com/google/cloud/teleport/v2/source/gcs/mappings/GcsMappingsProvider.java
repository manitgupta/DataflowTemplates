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
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

/**
 * Type handling of the GCS source, which reads the bulk migration Avro format.
 *
 * <p>The payload of every file already carries the unified Avro schema the template's own sink
 * wrote, so payload values are copied as they are, except that strings become {@link String}s and
 * JSON strings ({@code logicalType: json}) are canonicalized by {@link JsonNumberCanonicalizer}.
 * Column type names are derived from the Avro field schemas, for logging and the source schema.
 */
public final class GcsMappingsProvider {

  /** Avro logical type name of JSON strings in the unified Avro schema. */
  public static final String JSON_LOGICAL_TYPE = "json";

  private static final ImmutableMap<String, UnifiedMappingProvider.Type> TYPES =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("STRING", UnifiedMappingProvider.Type.STRING)
          .put("JSON", UnifiedMappingProvider.Type.JSON)
          .put("LONG", UnifiedMappingProvider.Type.LONG)
          .put("INTEGER", UnifiedMappingProvider.Type.INTEGER)
          .put("DOUBLE", UnifiedMappingProvider.Type.DOUBLE)
          .put("FLOAT", UnifiedMappingProvider.Type.FLOAT)
          .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          .put("BYTES", UnifiedMappingProvider.Type.BYTES)
          .build();

  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      TYPES.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  e -> e.getKey(), e -> UnifiedMappingProvider.getMapping(e.getValue())));

  private static final ImmutableMap<String, String> LOGICAL_TYPE_NAMES =
      ImmutableMap.<String, String>builder()
          .put(JSON_LOGICAL_TYPE, "JSON")
          .put("timestamp-micros", "TIMESTAMP")
          .put("timestamp-millis", "TIMESTAMP")
          .put("local-timestamp-micros", "TIMESTAMP")
          .put("local-timestamp-millis", "TIMESTAMP")
          .put("date", "DATE")
          .put("decimal", "DECIMAL")
          .put("time-micros", "TIME")
          .put("time-millis", "TIME")
          .put("uuid", "UUID")
          .build();

  private GcsMappingsProvider() {}

  /**
   * Returns the mapping from type name to unified type mapping for the type names this source
   * derives for common columns (see {@link #columnTypeName}).
   */
  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return MAPPING;
  }

  /**
   * Returns a {@link UnifiedTypeMapper} for {@link #getMapping()}. The GCS source itself adds
   * columns with their file's Avro schema, so the mapper is only needed to create the table schema
   * builder.
   */
  public static UnifiedTypeMapper typeMapper() {
    return new UnifiedTypeMapper(getMapping());
  }

  /**
   * Returns a readable type name for a payload field schema: the nullable union is unwrapped, a
   * known logical type gives its name (e.g. {@code JSON}, {@code TIMESTAMP}, {@code DECIMAL}),
   * otherwise the upper-case Avro type ({@code STRING}, {@code LONG}, {@code RECORD}, ...). A union
   * with several non-null branches is {@code UNION}.
   */
  public static String columnTypeName(Schema fieldSchema) {
    Schema schema = unwrapNullable(fieldSchema);
    if (schema.getType() == Schema.Type.UNION) {
      return "UNION";
    }
    String logicalName = logicalTypeName(schema);
    if (logicalName != null) {
      String known = LOGICAL_TYPE_NAMES.get(logicalName);
      return known != null ? known : logicalName.toUpperCase(Locale.ROOT);
    }
    if (schema.getType() == Schema.Type.INT) {
      return "INTEGER";
    }
    return schema.getType().getName().toUpperCase(Locale.ROOT);
  }

  /**
   * Returns the {@link SourceColumnType} for a payload field schema, named by {@link
   * #columnTypeName}.
   */
  public static SourceColumnType columnType(Schema fieldSchema) {
    return new SourceColumnType(columnTypeName(fieldSchema), new Long[] {}, new Long[] {});
  }

  /** True iff the (possibly nullable) field schema is a string with {@code logicalType: json}. */
  public static boolean isJson(Schema fieldSchema) {
    Schema schema = unwrapNullable(fieldSchema);
    return schema.getType() == Schema.Type.STRING
        && JSON_LOGICAL_TYPE.equals(logicalTypeName(schema));
  }

  /** True iff the (possibly nullable) field schema is a plain Avro string. */
  public static boolean isString(Schema fieldSchema) {
    return unwrapNullable(fieldSchema).getType() == Schema.Type.STRING;
  }

  /**
   * Converts a payload value read from a file to the value of the {@code SourceRow} payload: {@code
   * null} stays {@code null}, JSON strings are canonicalized, other strings become {@link String}s
   * and every other value is returned as is.
   *
   * @param fieldSchema schema of the payload field.
   * @param avroValue value read from the Avro file.
   */
  @Nullable
  public static Object toPayloadValue(Schema fieldSchema, @Nullable Object avroValue) {
    if (avroValue == null) {
      return null;
    }
    if (avroValue instanceof CharSequence) {
      if (isJson(fieldSchema)) {
        return JsonNumberCanonicalizer.canonicalize(avroValue.toString());
      }
      if (isString(fieldSchema)) {
        return avroValue.toString();
      }
    }
    return avroValue;
  }

  /** Returns the only non-null branch of a {@code [null, X]} union, else {@code schema}. */
  static Schema unwrapNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    List<Schema> nonNull =
        schema.getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .collect(Collectors.toList());
    return nonNull.size() == 1 ? nonNull.get(0) : schema;
  }

  @Nullable
  private static String logicalTypeName(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      return logicalType.getName();
    }
    // Logical types unknown to Avro (e.g. "json") are kept as a plain property.
    return schema.getProp(LogicalType.LOGICAL_TYPE_PROP);
  }
}
