package com.google.cloud.teleport.v2.templates;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Use partitions to fetch data.
 */
public class FetchPartitionDataDoFn extends DoFn<KV<String, Partition>, SourceRecord> {

  private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
  // Map<TableName, PartitionColumnName>
  private final Map<String, String> tableToPartitionColumn;
  private transient DataSource dataSource;

  public FetchPartitionDataDoFn(JdbcIO.DataSourceConfiguration dataSourceConfiguration, Map<String, String> tableToPartitionColumn) {
    this.dataSourceConfiguration = dataSourceConfiguration;
    this.tableToPartitionColumn = tableToPartitionColumn;
  }

  @Setup
  public void setup() {
    dataSource = dataSourceConfiguration.buildDatasource();
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

      try (ResultSet rs = statement.executeQuery()) {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
          // Convert each row into a generic Map
          Map<String, Object> row = new HashMap<>();
          for (int i = 1; i <= columnCount; i++) {
            row.put(metaData.getColumnName(i), rs.getObject(i));
          }
          c.output(new SourceRecord(row));
        }
      }
    }
  }
}
