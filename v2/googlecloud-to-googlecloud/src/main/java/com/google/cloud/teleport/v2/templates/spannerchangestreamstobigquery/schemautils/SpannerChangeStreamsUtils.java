/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link SpannerChangeStreamsUtils} provides methods that retrieve schema information from
 * Spanner. Note all the models returned in the methods of these class are tracked by the change
 * stream.
 */
public class SpannerChangeStreamsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsUtils.class);

  private static final String INFORMATION_SCHEMA_TABLE_NAME = "TABLE_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_TABLE_NAME = "table_name";
  private static final String INFORMATION_SCHEMA_COLUMN_NAME = "COLUMN_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_COLUMN_NAME = "column_name";
  private static final String INFORMATION_SCHEMA_SPANNER_TYPE = "SPANNER_TYPE";
  private static final String INFORMATION_SCHEMA_POSTGRES_SPANNER_TYPE = "spanner_type";
  private static final String INFORMATION_SCHEMA_ORDINAL_POSITION = "ORDINAL_POSITION";
  private static final String INFORMATION_SCHEMA_POSTGRES_ORDINAL_POSITION = "ordinal_position";
  private static final String INFORMATION_SCHEMA_CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_CONSTRAINT_NAME = "constraint_name";
  private static final String INFORMATION_SCHEMA_ALL = "ALL";
  private static final String INFORMATION_SCHEMA_POSTGRES_ALL = "all";

  private DatabaseClient databaseClient;
  private String changeStreamName;
  private Dialect dialect;
  private Timestamp bound;
  private RpcPriority rpcPriority;

  public SpannerChangeStreamsUtils(
      DatabaseClient databaseClient,
      String changeStreamName,
      Dialect dialect,
      RpcPriority rpcPriority,
      Timestamp bound) {
    this.databaseClient = databaseClient;
    this.changeStreamName = changeStreamName;
    this.dialect = dialect;
    this.bound = bound;
    this.rpcPriority = rpcPriority;
  }

  public SpannerChangeStreamsUtils(
      DatabaseClient databaseClient,
      String changeStreamName,
      Dialect dialect,
      RpcPriority rpcPriority) {
    this(databaseClient, changeStreamName, dialect, rpcPriority, null);
  }

  /**
   * @return a map where the key is the table name tracked by the change stream and the value is the
   *     {@link TrackedSpannerTable} object of the table name. This function should be called once
   *     in the initialization of the DoFn.
   */
  public Map<String, TrackedSpannerTable> getSpannerTableByName() {
    Set<String> spannerTableNames = getSpannerTableNamesTrackedByChangeStreams();

    Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName =
        getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName();

    return getSpannerTableByName(
        spannerTableNames, spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);
  }

  /**
   * @return a map where the key is the table name tracked by the change stream and the value is the
   *     {@link TrackedSpannerTable} object of the table name.
   */
  private Map<String, TrackedSpannerTable> getSpannerTableByName(
      Set<String> spannerTableNames,
      Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    Map<String, Map<String, Integer>> keyColumnNameToOrdinalPositionByTableName =
        getKeyColumnNameToOrdinalPositionByTableName(spannerTableNames);
    Map<String, List<TrackedSpannerColumn>> spannerColumnsByTableName =
        getSpannerColumnsByTableName(
            spannerTableNames,
            keyColumnNameToOrdinalPositionByTableName,
            spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);

    Map<String, TrackedSpannerTable> result = new HashMap<>();
    for (String tableName : spannerColumnsByTableName.keySet()) {
      List<TrackedSpannerColumn> pkColumns = new ArrayList<>();
      List<TrackedSpannerColumn> nonPkColumns = new ArrayList<>();
      Map<String, Integer> keyColumnNameToOrdinalPosition =
          keyColumnNameToOrdinalPositionByTableName.get(tableName);
      for (TrackedSpannerColumn spannerColumn : spannerColumnsByTableName.get(tableName)) {
        if (keyColumnNameToOrdinalPosition.containsKey(spannerColumn.getName())) {
          pkColumns.add(spannerColumn);
        } else {
          nonPkColumns.add(spannerColumn);
        }
      }
      result.put(tableName, new TrackedSpannerTable(tableName, pkColumns, nonPkColumns));
    }

    return result;
  }

  /**
   * Query INFORMATION_SCHEMA.COLUMNS to construct {@link SpannerColumn} for each Spanner column
   * tracked by change stream.
   */
  private Map<String, List<TrackedSpannerColumn>> getSpannerColumnsByTableName(
      Set<String> spannerTableNames,
      Map<String, Map<String, Integer>> keyColumnNameToOrdinalPositionByTableName,
      Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    Map<String, List<TrackedSpannerColumn>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
                + "FROM INFORMATION_SCHEMA.COLUMNS");
    if (this.isPostgres()) {
      // Skip the columns of the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
        sqlStringBuilder.append(
            spannerTableNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
        sqlStringBuilder.append("])");
      }
      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    } else {
      // Skip the columns of the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
      if (!spannerTableNames.isEmpty()) {
        statementBuilder
            .bind("tableNames")
            .to(Value.stringArray(new ArrayList<>(spannerTableNames)));
      }
    }

    try (ResultSet columnsResultSet =
        bound != null
            ? databaseClient
                .singleUse(TimestampBound.ofReadTimestamp(bound))
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))
            : databaseClient
                .singleUse()
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))) {
      while (columnsResultSet.next()) {
        String tableName = columnsResultSet.getString(informationSchemaTableName());
        String columnName = columnsResultSet.getString(informationSchemaColumnName());
        // Skip if the columns of the table is tracked explicitly, and the specified column is not
        // tracked. Primary key columns are always tracked.
        if (spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName.containsKey(tableName)
            && !spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName
                .get(tableName)
                .contains(columnName)
            && (!keyColumnNameToOrdinalPositionByTableName.containsKey(tableName)
                || !keyColumnNameToOrdinalPositionByTableName
                    .get(tableName)
                    .containsKey(columnName))) {
          continue;
        }

        int ordinalPosition = (int) columnsResultSet.getLong(informationSchemaOrdinalPosition());
        String spannerType = columnsResultSet.getString(informationSchemaSpannerType());
        result.putIfAbsent(tableName, new ArrayList<>());
        // Set primary key ordinal position for primary key column.
        int pkOrdinalPosition = -1;
        if (!keyColumnNameToOrdinalPositionByTableName.containsKey(tableName)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot find key column for change stream %s and table %s",
                  changeStreamName, tableName));
        }
        if (keyColumnNameToOrdinalPositionByTableName.get(tableName).containsKey(columnName)) {
          pkOrdinalPosition =
              keyColumnNameToOrdinalPositionByTableName.get(tableName).get(columnName);
        }
        TrackedSpannerColumn spannerColumn =
            TrackedSpannerColumn.create(
                columnName,
                informationSchemaTypeToSpannerType(spannerType),
                ordinalPosition,
                pkOrdinalPosition);
        result.get(tableName).add(spannerColumn);
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Caught exception when constructing SpannerColumn for each column tracked by change"
                  + " stream %s, message: %s, cause: %s",
              changeStreamName, Optional.ofNullable(e.getMessage()), e.getCause());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }

    return result;
  }

  /**
   * Query INFORMATION_SCHEMA.KEY_COLUMN_USAGE to get the names of the primary key columns that are
   * tracked by change stream. We need to know the primary keys information to be able to set {@link
   * TableRow} or do Spanner snapshot read, the alternative way is to extract the primary key
   * information from {@link Mod} whenever we process it, but it's less efficient, since that will
   * require to parse the types and sort them based on the ordinal positions for each {@link Mod}.
   */
  private Map<String, Map<String, Integer>> getKeyColumnNameToOrdinalPositionByTableName(
      Set<String> spannerTableNames) {
    Map<String, Map<String, Integer>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      StringBuilder sqlStringBuilder =
          new StringBuilder(
              "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                  + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

      // Skip the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
        sqlStringBuilder.append(
            spannerTableNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
        sqlStringBuilder.append("])");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    } else {
      StringBuilder sqlStringBuilder =
          new StringBuilder(
              "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                  + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

      // Skip the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
      if (!spannerTableNames.isEmpty()) {
        statementBuilder
            .bind("tableNames")
            .to(Value.stringArray(new ArrayList<>(spannerTableNames)));
      }
    }

    try (ResultSet keyColumnsResultSet =
        bound != null
            ? databaseClient
                .singleUse(TimestampBound.ofReadTimestamp(bound))
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))
            : databaseClient
                .singleUse()
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))) {
      while (keyColumnsResultSet.next()) {
        String tableName = keyColumnsResultSet.getString(informationSchemaTableName());
        String columnName = keyColumnsResultSet.getString(informationSchemaColumnName());
        // Note The ordinal position of primary key in INFORMATION_SCHEMA.KEY_COLUMN_USAGE table
        // is different from the ordinal position from INFORMATION_SCHEMA.COLUMNS table.
        int ordinalPosition = (int) keyColumnsResultSet.getLong(informationSchemaOrdinalPosition());
        String constraintName = keyColumnsResultSet.getString(informationSchemaConstraintName());
        // We are only interested in primary key constraint.
        if (isPrimaryKey(constraintName)) {
          result.putIfAbsent(tableName, new HashMap<>());
          result.get(tableName).put(columnName, ordinalPosition);
        }
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Caught exception when getting key columns names of tables tracked change stream %s,"
                  + " message: %s, cause: %s",
              changeStreamName, Optional.ofNullable(e.getMessage()), e.getCause());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }

    return result;
  }

  private static boolean isPrimaryKey(String constraintName) {
    return constraintName.startsWith("PK");
  }

  /**
   * @return the Spanner table names that are tracked by the change stream.
   */
  private Set<String> getSpannerTableNamesTrackedByChangeStreams() {
    boolean isChangeStreamForAll = isChangeStreamForAll();

    Statement.Builder statementBuilder;
    String sql;
    if (this.isPostgres()) {
      sql =
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      sql =
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }

    if (isChangeStreamForAll) {
      // If the change stream is tracking all tables, we have to look up the table names in
      // INFORMATION_SCHEMA.TABLES.
      if (this.isPostgres()) {
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'";
      } else {
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";
      }
      statementBuilder = Statement.newBuilder(sql);
    }

    Set<String> result = new HashSet<>();
    try (ResultSet resultSet =
        bound != null
            ? databaseClient
                .singleUse(TimestampBound.ofReadTimestamp(bound))
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))
            : databaseClient
                .singleUse()
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))) {

      while (resultSet.next()) {
        result.add(resultSet.getString(informationSchemaTableName()));
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Caught exception when reading table names tracked by change stream %s,"
                  + " message: %s, cause: %s",
              changeStreamName, Optional.ofNullable(e.getMessage()), e.getCause());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }

    return result;
  }

  /**
   * @return if the change stream tracks all the tables in the database.
   */
  private boolean isChangeStreamForAll() {
    Boolean result = null;

    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      String sql =
          "SELECT CHANGE_STREAMS.all FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      String sql =
          "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }
    try (ResultSet resultSet =
        bound != null
            ? databaseClient
                .singleUse(TimestampBound.ofReadTimestamp(bound))
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))
            : databaseClient
                .singleUse()
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))) {
      while (resultSet.next()) {
        if (this.isPostgres()) {
          String resultString = resultSet.getString(informationSchemaAll());
          if (resultString != null) {
            result = resultSet.getString(informationSchemaAll()).equalsIgnoreCase("YES");
          }
        } else {
          result = resultSet.getBoolean(informationSchemaAll());
        }
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Caught exception when reading change stream %s is tracking FOR ALL, message: %s,"
                  + " cause: %s",
              changeStreamName, Optional.ofNullable(e.getMessage()), e.getCause());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }

    if (result == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find change stream %s in INFORMATION_SCHEMA", changeStreamName));
    }

    return result;
  }

  /**
   * @return the Spanner column names that are tracked explicitly by change stream by table name.
   *     e.g. Given Singers table with SingerId, FirstName and LastName, an empty map will be
   *     returned if we have change stream "CREATE CHANGE STREAM AllStream FOR ALL" or "CREATE
   *     CHANGE STREAM AllStream FOR Singers", {"Singers" -> {"SingerId", "FirstName"}} will be
   *     returned if we have change stream "CREATE CHANGE STREAM SingerStream FOR Singers(SingerId,
   *     FirstName)"
   */
  private Map<String, Set<String>>
      getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName() {
    Map<String, Set<String>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      String sql =
          "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      String sql =
          "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }

    try (ResultSet resultSet =
        bound != null
            ? databaseClient
                .singleUse(TimestampBound.ofReadTimestamp(bound))
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))
            : databaseClient
                .singleUse()
                .executeQuery(statementBuilder.build(), Options.priority(rpcPriority))) {

      while (resultSet.next()) {
        String tableName = resultSet.getString(informationSchemaTableName());
        String columnName = resultSet.getString(informationSchemaColumnName());
        result.putIfAbsent(tableName, new HashSet<>());
        result.get(tableName).add(columnName);
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Caught exception when reading column names tracked by change"
                  + " stream %s, message: %s, cause: %s",
              changeStreamName, Optional.ofNullable(e.getMessage()), e.getCause());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage, e);
    }

    return result;
  }

  private Type informationSchemaTypeToSpannerType(String type) {
    if (this.isPostgres()) {
      return TypesUtils.informationSchemaPostgreSQLTypeToSpannerType(type);
    }
    return TypesUtils.informationSchemaGoogleSQLTypeToSpannerType(type);
  }

  public static void appendToSpannerKey(
      TrackedSpannerColumn column, JSONObject keysJsonObject, Key.Builder keyBuilder) {
    Type.Code code = column.getType().getCode();
    String name = column.getName();
    switch (code) {
      case BOOL:
        keyBuilder.append(keysJsonObject.getBoolean(name));
        break;
      case FLOAT64:
        keyBuilder.append(keysJsonObject.getDouble(name));
        break;
      case INT64:
        keyBuilder.append(keysJsonObject.getLong(name));
        break;
      case NUMERIC:
        keyBuilder.append(keysJsonObject.getBigDecimal(name));
        break;
      case BYTES:
      case DATE:
      case STRING:
      case TIMESTAMP:
        keyBuilder.append(keysJsonObject.getString(name));
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", code));
    }
  }

  private boolean isPostgres() {
    return this.dialect == Dialect.POSTGRESQL;
  }

  private String informationSchemaTableName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_TABLE_NAME;
    }
    return INFORMATION_SCHEMA_TABLE_NAME;
  }

  private String informationSchemaColumnName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_COLUMN_NAME;
    }
    return INFORMATION_SCHEMA_COLUMN_NAME;
  }

  private String informationSchemaSpannerType() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_SPANNER_TYPE;
    }
    return INFORMATION_SCHEMA_SPANNER_TYPE;
  }

  private String informationSchemaOrdinalPosition() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_ORDINAL_POSITION;
    }
    return INFORMATION_SCHEMA_ORDINAL_POSITION;
  }

  private String informationSchemaConstraintName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_CONSTRAINT_NAME;
    }
    return INFORMATION_SCHEMA_CONSTRAINT_NAME;
  }

  private String informationSchemaAll() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_ALL;
    }
    return INFORMATION_SCHEMA_ALL;
  }
}
