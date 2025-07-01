package com.google.cloud.teleport.v2.templates;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use partitions to fetch data.
 */
public class FetchPartitionDataDoFn extends DoFn<KV<String, Partition>, SourceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(FetchPartitionDataDoFn.class);
  private final SerializableFunction<Void, DataSource> hikariPoolableDataSourceProvider;
  // Map<TableName, PartitionColumnName>
  private final Map<String, String> tableToPartitionColumn;
  private transient DataSource dataSource;

  public FetchPartitionDataDoFn(SerializableFunction<Void, DataSource> hikariPoolableDataSourceProvider, Map<String, String> tableToPartitionColumn) {
    this.hikariPoolableDataSourceProvider = hikariPoolableDataSourceProvider;
    this.tableToPartitionColumn = tableToPartitionColumn;
  }

  @Setup
  public void setup() {
    dataSource = hikariPoolableDataSourceProvider.apply(null);
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String tableName = c.element().getKey();
    Partition partition = c.element().getValue();
    String partitionColumn = tableToPartitionColumn.get(tableName);

    if (partitionColumn == null) {
      throw new IllegalArgumentException("No partition column found for table: " + tableName);
    }

    // Add the source_table as a literal column to identify the origin of the row
    String query = String.format("SELECT *, '%s' AS source_table FROM %s WHERE %s >= ? AND %s < ?",
        tableName, tableName, partitionColumn, partitionColumn);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(query)) {

      statement.setObject(1, partition.lowerBound);
      statement.setObject(2, partition.upperBound);

      try (ResultSet resultSet = statement.executeQuery()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();

        while (resultSet.next()) {
          // Convert each row into a generic Map
          SourceRecord sourceRecord = new SourceRecord();
          for (int colOrdinal = 1; colOrdinal <= columnCount; colOrdinal++) {
            int type = resultSetMetaData.getColumnType(colOrdinal);
            switch (type) {
              case Types.CHAR:
              case Types.VARCHAR:
              case Types.LONGVARCHAR:
              case Types.OTHER:
                String stringVal = resultSet.getString(colOrdinal);
                if (stringVal != null && !resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), stringVal);
                }
                break;
              case Types.INTEGER:
                Integer intVal = resultSet.getInt(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), intVal);
                }
                break;
              case Types.DOUBLE:
                Double doubleVal = resultSet.getDouble(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), doubleVal);
                }
                break;
              case Types.REAL:
                Float floatVal = resultSet.getFloat(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), floatVal);
                }
                break;
              case Types.TINYINT:
                Short shortVal = resultSet.getShort(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), shortVal);
                }
                break;
              case Types.BIGINT:
                Long longVal = resultSet.getLong(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), longVal);
                }
                break;
              case Types.DECIMAL:
                BigDecimal decimalVal = resultSet.getBigDecimal(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), decimalVal);
                }
                break;
              case Types.BIT:
              case Types.BOOLEAN:
                Boolean boolVal = resultSet.getBoolean(colOrdinal);
                if (!resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), boolVal);
                }
                break;
              case Types.BINARY:
              case Types.VARBINARY:
              case Types.LONGVARBINARY:
                byte[] bytesVal = resultSet.getBytes(colOrdinal);
                if (bytesVal != null && !resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), bytesVal);
                }
                break;
              case Types.DATE:
                Date dateVal = resultSet.getDate(colOrdinal);
                if (dateVal != null && !resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), dateVal);
                }
                break;
              case Types.TIMESTAMP:
              case Types.TIME_WITH_TIMEZONE:
                java.sql.Timestamp timestampVal = resultSet.getTimestamp(colOrdinal);
                if (timestampVal != null && !resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), timestampVal);
                }
                break;
              case Types.ARRAY:
                Array arrayVal = resultSet.getArray(colOrdinal);
                if (arrayVal != null && !resultSet.wasNull()) {
                  sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                      resultSetMetaData.getColumnTypeName(colOrdinal), arrayVal);
                }
                break;
              default:
                LOG.error("Unsupported type: {}", type);
                throw new RuntimeException(String.format("Unsupported type: %d", type));
            }
          }
          c.output(sourceRecord);
        }
      }
    }
  }
}
