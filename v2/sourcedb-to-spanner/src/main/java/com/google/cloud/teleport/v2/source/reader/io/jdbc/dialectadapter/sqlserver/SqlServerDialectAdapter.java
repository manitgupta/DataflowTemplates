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

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter for SQLServer dialect of JDBC databases. */
public final class SqlServerDialectAdapter implements DialectAdapter {

  private static final Logger logger = LoggerFactory.getLogger(SqlServerDialectAdapter.class);

  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info("Discovering tables for DataSource: {}", dataSource);
    // Note: SQL Server uses "BASE TABLE" in TABLE_TYPE
    String query =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'";
    // TODO: Support schemas other than dbo if needed, for now assume dbo or pass
    // via options if supported.
    // Actually sourceSchemaReference might have it? sourceSchemaReference.dbName()
    // is usually the DB name.
    // The Namespace in JdbcSchemaReference could be the schema (e.g. 'dbo').
    // Let's modify to use schema if provided, or default to dbo?
    // sourceSchemaReference.jdbc().namespace() might be null or empty.

    // Let's rely on TABLE_CATALOG = dbName if needed, but usually connection is
    // already to the DB.
    // If we want to filter by Schema (Namespace), we should check if it's in
    // sourceSchemaReference.
    // JdbcSchemaReference has 'namespace'.

    // For now, I'll restrict to TABLE_TYPE='BASE TABLE' and filter by Catalog if
    // possible, but standard is usually just TABLE_NAME.

    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (Statement stmt = dataSource.getConnection().createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        tablesBuilder.add(rs.getString("TABLE_NAME"));
      }
      return tablesBuilder.build();
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {

    String discoveryQuery =
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
            + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?";

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> tableSchemaBuilder =
        ImmutableMap.builder();

    try (PreparedStatement statement =
        dataSource.getConnection().prepareStatement(discoveryQuery)) {
      for (String table : tables) {
        statement.setString(1, table);
        ResultSet rs = statement.executeQuery();
        ImmutableMap.Builder<String, SourceColumnType> colsBuilder = ImmutableMap.builder();
        while (rs.next()) {
          String colName = rs.getString("COLUMN_NAME");
          String dataType = rs.getString("DATA_TYPE");
          Long charMaxLen =
              rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null
                  ? rs.getLong("CHARACTER_MAXIMUM_LENGTH")
                  : null;
          Long precision =
              rs.getObject("NUMERIC_PRECISION") != null ? rs.getLong("NUMERIC_PRECISION") : null;
          Long scale = rs.getObject("NUMERIC_SCALE") != null ? rs.getLong("NUMERIC_SCALE") : null;

          Long[] args = new Long[] {};
          if (charMaxLen != null) {
            args = new Long[] {charMaxLen};
          } else if (precision != null && scale != null) {
            args = new Long[] {precision, scale};
          } else if (precision != null) {
            args = new Long[] {precision};
          }

          colsBuilder.put(colName, new SourceColumnType(dataType.toUpperCase(), args, null));
        }
        tableSchemaBuilder.put(table, colsBuilder.build());
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    return tableSchemaBuilder.build();
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {

    // Using sys.indexes and friends for MSSQL
    String query =
        "SELECT "
            + "t.name AS TableName, "
            + "ind.name AS IndexName, "
            + "col.name AS ColumnName, "
            + "ind.is_unique AS IsUnique, "
            + "ind.is_primary_key AS IsPrimaryKey, "
            + "ic.key_ordinal AS KeyOrdinal, "
            + "col.max_length AS MaxLength, "
            + "col.precision AS Precision, "
            + "col.scale AS Scale, "
            + "typ.name AS DataTypeName "
            + "FROM sys.indexes ind "
            + "INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id "
            + "INNER JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id "
            + "INNER JOIN sys.tables t ON ind.object_id = t.object_id "
            + "INNER JOIN sys.types typ ON col.user_type_id = typ.user_type_id "
            + "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            + "WHERE t.name = ? AND s.name = 'dbo'";

    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> tableIndexesBuilder =
        ImmutableMap.builder();

    try (PreparedStatement statement = dataSource.getConnection().prepareStatement(query)) {
      for (String table : tables) {
        statement.setString(1, table);
        ResultSet rs = statement.executeQuery();
        ImmutableList.Builder<SourceColumnIndexInfo> indexesBuilder = ImmutableList.builder();
        while (rs.next()) {
          String indexName = rs.getString("IndexName");
          String columnName = rs.getString("ColumnName");
          boolean isUnique = rs.getBoolean("IsUnique");
          boolean isPrimary = rs.getBoolean("IsPrimaryKey");
          int ordinal = rs.getInt("KeyOrdinal");
          String typeName = rs.getString("DataTypeName").toUpperCase();

          IndexType indexType = IndexType.OTHER;
          if (typeName.contains("INT")) {
            indexType = IndexType.NUMERIC;
          } else if (typeName.contains("CHAR") || typeName.contains("TEXT")) {
            indexType = IndexType.STRING;
          } else if (typeName.contains("DATE") || typeName.contains("TIME")) {
            indexType = IndexType.TIME_STAMP;
          } else if (typeName.contains("DECIMAL") || typeName.contains("NUMERIC")) {
            indexType = IndexType.DECIMAL;
          }
          // Add more mappings as needed

          indexesBuilder.add(
              SourceColumnIndexInfo.builder()
                  .setColumnName(columnName)
                  .setIndexName(indexName)
                  .setIsUnique(isUnique)
                  .setIsPrimary(isPrimary)
                  .setOrdinalPosition((long) ordinal)
                  .setIndexType(indexType)
                  .build());
        }
        tableIndexesBuilder.put(table, indexesBuilder.build());
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    return tableIndexesBuilder.build();
  }

  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    return addWhereClause("SELECT * FROM " + tableName, partitionColumns);
  }

  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    // MSSQL does not support execution time limit directly in SELECT without
    // specific hints or resource governor
    // Query Store usage or SET STATEMENT_TERMINATOR.
    // We'll ignore timeoutMillis in the query string for now or use 'OPTION
    // (MAXRECURSION 0)' dummy?
    // Actually MSSQL has query hints. 'OPTION (MAXDOP 1)' etc.
    // For implementation simplicity, we return standard count for now.
    return addWhereClause("SELECT COUNT(*) FROM " + tableName, partitionColumns);
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    return addWhereClause(
        String.format("SELECT MIN(%s), MAX(%s) FROM %s", colName, colName, tableName),
        partitionColumns);
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    // Basic timeout check for MSSQL
    return exception.getErrorCode() == -2; // Standard JDBC timeout? Or specific MSSQL codes.
    // We can iterate on this later.
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    // TODO: Implement for MSSQL if needed for text partitioning
    // Returning null or throwing might break if uniform splitter is used on text
    // columns
    // For now, return a dummy or simple query if possible.
    // MSSQL collation handling is complex.
    return "SELECT 'DUMMY'";
  }

  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {
    StringBuilder queryBuilder = new StringBuilder(query);
    boolean firstDone = false;
    for (String partitionColumn : partitionColumns) {
      if (firstDone) {
        queryBuilder.append(" AND ");
      } else {
        queryBuilder.append(" WHERE ");
      }
      queryBuilder.append("((? = 0) OR ");
      queryBuilder.append(
          String.format("(%1$s >= ? AND (%1$s < ? OR (? = 1 AND %1$s = ?)))", partitionColumn));
      queryBuilder.append(")");
      firstDone = true;
    }
    return queryBuilder.toString();
  }
}
