/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Provides a set of {@link org.apache.avro.Schema Avro Schemas} that each of the SQL Server
 * database's type must map into.
 */
public final class SqlServerMappingProvider {
  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("BIGINT", UnifiedMappingProvider.Type.LONG)
          .put("BINARY", UnifiedMappingProvider.Type.BYTES)
          .put(
              "BIT",
              UnifiedMappingProvider.Type
                  .BOOLEAN) // SQL Server BIT is mostly boolean 0/1, mapping to boolean is safe for
          // Avro if we handle it
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          .put("DATETIME", UnifiedMappingProvider.Type.DATETIME)
          .put("DATETIME2", UnifiedMappingProvider.Type.DATETIME)
          .put("DATETIMEOFFSET", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DECIMAL", UnifiedMappingProvider.Type.DECIMAL)
          .put("FLOAT", UnifiedMappingProvider.Type.DOUBLE)
          .put("IMAGE", UnifiedMappingProvider.Type.BYTES) // Mapped to bytes
          .put("INT", UnifiedMappingProvider.Type.INTEGER)
          .put("MONEY", UnifiedMappingProvider.Type.DECIMAL) // Money is decimal-like
          .put("NCHAR", UnifiedMappingProvider.Type.STRING)
          .put("NTEXT", UnifiedMappingProvider.Type.STRING)
          .put("NUMERIC", UnifiedMappingProvider.Type.DECIMAL)
          .put("NVARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("REAL", UnifiedMappingProvider.Type.FLOAT)
          .put("SMALLDATETIME", UnifiedMappingProvider.Type.DATETIME) // or Timestamp?
          .put("SMALLINT", UnifiedMappingProvider.Type.INTEGER)
          .put("SMALLMONEY", UnifiedMappingProvider.Type.DECIMAL)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          .put("TIME", UnifiedMappingProvider.Type.STRING)
          .put(
              "TIMESTAMP",
              UnifiedMappingProvider.Type
                  .LONG) // SQL Server TIMESTAMP is binary rowversion, unrelated to time.
          .put("TINYINT", UnifiedMappingProvider.Type.INTEGER)
          .put("UNIQUEIDENTIFIER", UnifiedMappingProvider.Type.STRING)
          .put("VARBINARY", UnifiedMappingProvider.Type.BYTES)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("XML", UnifiedMappingProvider.Type.STRING)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return MAPPING;
  }

  private SqlServerMappingProvider() {}
}
