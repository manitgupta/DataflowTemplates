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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

/** Test class for {@link SqlServerDialectAdapter}. */
@RunWith(MockitoJUnitRunner.class)
public class SqlServerDialectAdapterTest {
  @Mock DataSource mockDataSource;
  @Mock Connection mockConnection;
  @Mock PreparedStatement mockPreparedStatement;
  @Mock Statement mockStatement;

  @Test
  public void testDiscoverTables() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> testTables = ImmutableList.of("testTable1", "testTable2");

    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

    OngoingStubbing<Boolean> stubNext = when(mockResultSet.next());
    for (String ignored : testTables) {
      stubNext = stubNext.thenReturn(true);
    }
    stubNext.thenReturn(false);

    OngoingStubbing<String> stubGetString = when(mockResultSet.getString("TABLE_NAME"));
    for (String tbl : testTables) {
      stubGetString = stubGetString.thenReturn(tbl);
    }

    ImmutableList<String> tables =
        new SqlServerDialectAdapter().discoverTables(mockDataSource, sourceSchemaReference);

    assertThat(tables).isEqualTo(testTables);
  }

  @Test
  public void testDiscoverTableSchema() throws SQLException, RetriableSchemaDiscoveryException {
    final String testTable = "testTable";
    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();
    final ResultSet mockResultSet = getMockInfoSchemaRs();

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doNothing().when(mockPreparedStatement).setString(1, testTable);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    assertThat(
            new SqlServerDialectAdapter()
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)))
        .isEqualTo(getExpectedColumnMapping(testTable));
  }

  @Test
  public void testDiscoverTableSchemaExceptions() throws SQLException {
    final String testTable = "testTable";
    final JdbcSchemaReference sourceSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection()).thenThrow(new SQLException("test"));
    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new SqlServerDialectAdapter()
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  private static ResultSet getMockInfoSchemaRs() throws SQLException {
    return new MockRSBuilder(
            MockInformationSchema.builder()
                .withColName("int_col")
                .withDataType("int")
                .withCharMaxLength(null)
                .withNumericPrecision(10L)
                .withNumericScale(0L)
                .withColName("varchar_col")
                .withDataType("varchar")
                .withCharMaxLength(100L)
                .withNumericPrecision(null)
                .withNumericScale(null)
                .withColName("decimal_col")
                .withDataType("decimal")
                .withCharMaxLength(null)
                .withNumericPrecision(10L)
                .withNumericScale(5L)
                .build())
        .createMock();
  }

  private static ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      getExpectedColumnMapping(String testTable) {
    return ImmutableMap.of(
        testTable,
        ImmutableMap.<String, SourceColumnType>builder()
            .put("int_col", new SourceColumnType("INT", new Long[] {10L, 0L}, null))
            .put("varchar_col", new SourceColumnType("VARCHAR", new Long[] {100L}, null))
            .put("decimal_col", new SourceColumnType("DECIMAL", new Long[] {10L, 5L}, null))
            .build());
  }

  @Test
  public void testGetReadQuery() {
    String testTable = "testTable";
    ImmutableList<String> cols = ImmutableList.of("col_1", "col_2");
    // Explicitly check for SqlServer syntax if relevant, or just the WHERE clause
    // logic
    // SqlServerDialectAdapter implementation uses "SELECT * FROM tableName WHERE
    // ..."
    // distinct from other adapters usually? No, the logic in addWhereClause is
    // similar.
    // However, verify the logic matches exactly what we implemented.
    String expected =
        "SELECT * FROM testTable WHERE ((? = 0) OR (col_1 >= ? AND (col_1 < ? OR (? = 1 AND col_1 = ?)))) AND ((? = 0) OR (col_2 >= ? AND (col_2 < ? OR (? = 1 AND col_2 = ?))))";
    assertThat(new SqlServerDialectAdapter().getReadQuery(testTable, cols)).isEqualTo(expected);
  }
}

class MockRSBuilder {
  private final MockInformationSchema schema;
  private int rowIndex;
  private Boolean wasNull = null;

  MockRSBuilder(MockInformationSchema schema) {
    this.schema = schema;
    this.rowIndex = -1;
  }

  ResultSet createMock() throws SQLException {
    final var rs = mock(ResultSet.class);

    doAnswer(
            invocation -> {
              rowIndex = rowIndex + 1;
              wasNull = null;
              return rowIndex < schema.colNames().size();
            })
        .when(rs)
        .next();

    doAnswer(
            invocation -> {
              wasNull = null;
              return schema.colNames().get(rowIndex);
            })
        .when(rs)
        .getString("COLUMN_NAME");

    doAnswer(
            invocation -> {
              wasNull = null;
              return schema.dataTypes().get(rowIndex);
            })
        .when(rs)
        .getString("DATA_TYPE");

    // Mock getObject for Long values to handle nulls correctly as implemented in
    // adapter
    doAnswer(
            invocation -> {
              if (schema.charMaxLengthWasNulls().get(rowIndex)) {
                return null;
              }
              return schema.charMaxLengths().get(rowIndex);
            })
        .when(rs)
        .getObject("CHARACTER_MAXIMUM_LENGTH");

    doAnswer(
            invocation -> {
              if (schema.numericPrecisionWasNulls().get(rowIndex)) {
                return null;
              }
              return schema.numericPrecisions().get(rowIndex);
            })
        .when(rs)
        .getObject("NUMERIC_PRECISION");

    doAnswer(
            invocation -> {
              if (schema.numericScaleWasNulls().get(rowIndex)) {
                return null;
              }
              return schema.numericScales().get(rowIndex);
            })
        .when(rs)
        .getObject("NUMERIC_SCALE");

    // Also mock getLong just in case, though adapter uses getObject for these check
    doAnswer(
            invocation -> {
              if (schema.charMaxLengthWasNulls().get(rowIndex)) {
                return 0L;
              }
              return schema.charMaxLengths().get(rowIndex);
            })
        .when(rs)
        .getLong("CHARACTER_MAXIMUM_LENGTH");

    doAnswer(
            invocation -> {
              if (schema.numericPrecisionWasNulls().get(rowIndex)) {
                return 0L;
              }
              return schema.numericPrecisions().get(rowIndex);
            })
        .when(rs)
        .getLong("NUMERIC_PRECISION");

    doAnswer(
            invocation -> {
              if (schema.numericScaleWasNulls().get(rowIndex)) {
                return 0L;
              }
              return schema.numericScales().get(rowIndex);
            })
        .when(rs)
        .getLong("NUMERIC_SCALE");

    return rs;
  }
}

@AutoValue
abstract class MockInformationSchema {
  abstract ImmutableList<String> colNames();

  abstract ImmutableList<String> dataTypes();

  abstract ImmutableList<Long> charMaxLengths();

  abstract ImmutableList<Boolean> charMaxLengthWasNulls();

  abstract ImmutableList<Long> numericPrecisions();

  abstract ImmutableList<Boolean> numericPrecisionWasNulls();

  abstract ImmutableList<Long> numericScales();

  abstract ImmutableList<Boolean> numericScaleWasNulls();

  public static Builder builder() {
    return new AutoValue_MockInformationSchema.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract ImmutableList.Builder<String> colNamesBuilder();

    abstract ImmutableList.Builder<String> dataTypesBuilder();

    abstract ImmutableList.Builder<Long> charMaxLengthsBuilder();

    abstract ImmutableList.Builder<Boolean> charMaxLengthWasNullsBuilder();

    abstract ImmutableList.Builder<Long> numericPrecisionsBuilder();

    abstract ImmutableList.Builder<Boolean> numericPrecisionWasNullsBuilder();

    abstract ImmutableList.Builder<Long> numericScalesBuilder();

    abstract ImmutableList.Builder<Boolean> numericScaleWasNullsBuilder();

    public Builder withColName(String colName) {
      this.colNamesBuilder().add(colName);
      return this;
    }

    public Builder withDataType(String dataType) {
      this.dataTypesBuilder().add(dataType);
      return this;
    }

    public Builder withCharMaxLength(Long charMaxLength) {
      if (charMaxLength == null) {
        this.charMaxLengthsBuilder().add(0L);
        this.charMaxLengthWasNullsBuilder().add(true);
      } else {
        this.charMaxLengthsBuilder().add(charMaxLength);
        this.charMaxLengthWasNullsBuilder().add(false);
      }
      return this;
    }

    public Builder withNumericPrecision(Long precision) {
      if (precision == null) {
        this.numericPrecisionsBuilder().add(0L);
        this.numericPrecisionWasNullsBuilder().add(true);
      } else {
        this.numericPrecisionsBuilder().add(precision);
        this.numericPrecisionWasNullsBuilder().add(false);
      }
      return this;
    }

    public Builder withNumericScale(Long scale) {
      if (scale == null) {
        this.numericScalesBuilder().add(0L);
        this.numericScaleWasNullsBuilder().add(true);
      } else {
        this.numericScalesBuilder().add(scale);
        this.numericScaleWasNullsBuilder().add(false);
      }
      return this;
    }

    public abstract MockInformationSchema build();
  }
}
