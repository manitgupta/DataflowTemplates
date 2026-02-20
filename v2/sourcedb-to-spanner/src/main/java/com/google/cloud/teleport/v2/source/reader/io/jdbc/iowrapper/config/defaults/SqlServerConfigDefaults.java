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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.sqlserver.SqlServerDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.SqlServerJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

/**
 * SQL Server Default Configuration for {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcIoWrapper JdbcIoWrapper}.
 */
public class SqlServerConfigDefaults {

  public static final MapperType DEFAULT_SQLSERVER_SCHEMA_MAPPER_TYPE = MapperType.SQLSERVER;
  // Actually MapperType.MYSQL might be specific to MySQL types.
  // UnifiedTypeMapper has MapperType.
  // I should check `UnifiedTypeMapper` to see if I need to add SQLSERVER there
  // too.
  // For now I will use MYSQL and if it fails I'll fix it.
  // Wait, I should probably add SQLSERVER to MapperType in UnifiedTypeMapper if
  // it doesn't exist.
  // I'll check UnifiedTypeMapper in a moment.

  public static final DialectAdapter DEFAULT_SQLSERVER_DIALECT_ADAPTER =
      new SqlServerDialectAdapter();
  public static final JdbcValueMappingsProvider DEFAULT_SQLSERVER_VALUE_MAPPING_PROVIDER =
      new SqlServerJdbcValueMappings();

  public static final Long DEFAULT_SQLSERVER_MAX_CONNECTIONS = 160L;

  public static final FluentBackoff DEFAULT_SQLSERVER_SCHEMA_DISCOVERY_BACKOFF =
      FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));

  /** Default Initialization Sequence for the JDBC connection. */
  public static final ImmutableList<String> DEFAULT_SQLSERVER_INIT_SEQ =
      ImmutableList.of(
          "SET LOCK_TIMEOUT 5000" // Example: 5 seconds lock timeout
          );

  private SqlServerConfigDefaults() {}
}
