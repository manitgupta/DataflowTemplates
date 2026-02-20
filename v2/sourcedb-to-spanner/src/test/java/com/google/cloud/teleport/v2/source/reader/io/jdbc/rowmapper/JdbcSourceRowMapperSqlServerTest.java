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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.SqlServerJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.sql.ResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcSourceRowMapperSqlServerTest {

  @Test
  public void testSqlServerIntMapping() throws Exception {
    // 1. Mock ResultSet
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInt("OrderID")).thenReturn(12345);
    when(resultSet.wasNull()).thenReturn(false);

    // 2. Prepare Schema
    String colName = "OrderID";
    String colType = "INT";
    SourceColumnType sourceColumnType = new SourceColumnType(colType, new Long[] {}, null);

    SourceTableSchema sourceTableSchema =
        SourceTableSchema.builder(UnifiedTypeMapper.MapperType.SQLSERVER)
            .setTableName("Orders")
            .addSourceColumnNameToSourceColumnType(colName, sourceColumnType)
            .build();

    JdbcSchemaReference jdbcSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();
    SourceSchemaReference sourceSchemaReference = SourceSchemaReference.ofJdbc(jdbcSchemaReference);

    // 3. Initialize Mapper
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(
            new SqlServerJdbcValueMappings(), sourceSchemaReference, sourceTableSchema, "shard1");

    // 4. Map Row
    SourceRow row = mapper.mapRow(resultSet);

    // 5. Verify
    Assert.assertNotNull(row);
    Assert.assertEquals(12345, row.getPayload().get(colName));
    Assert.assertTrue(row.getPayload().get(colName) instanceof Integer);
  }
}
