package com.google.cloud.teleport.v2.templates;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Generates partition ranges for a table using a fast, approximate row count.
 */
public class GeneratePartitionsFastFn extends DoFn<KV<String, String>, KV<String, Partition>> {

  private final SerializableFunction<Void, DataSource> hikariPoolableDataSourceProvider;
  private final String databaseName; // Required for INFORMATION_SCHEMA query

  public GeneratePartitionsFastFn(SerializableFunction<Void, DataSource> hikariPoolableDataSourceProvider, String databaseName) {
    this.hikariPoolableDataSourceProvider = hikariPoolableDataSourceProvider;
    this.databaseName = databaseName;
  }

  private transient DataSource dataSource;

  @Setup
  public void setup() {
    dataSource = hikariPoolableDataSourceProvider.apply(null);
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String tableName = c.element().getKey();
    String partitionColumn = c.element().getValue();

    long min = 0;
    long max = 0;
    long approximateCount = 0;

    // --- Step 1: Get approximate count from INFORMATION_SCHEMA (for MySQL) ---
    String countQuery = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(countQuery)) {
      ps.setString(1, databaseName);
      ps.setString(2, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          approximateCount = rs.getLong(1);
        }
      }
    }

    // --- Step 2: Get min and max values (this is usually fast on an indexed column) ---
    String minMaxQuery = String.format("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, tableName);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(minMaxQuery);
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        min = rs.getLong(1);
        max = rs.getLong(2);
      }
    }

    // --- Step 3: Calculate partitions based on the approximate count ---
    long numPartitions;
    if (approximateCount > 0) {
      // Logic from JdbcIO: Use the approximate count to determine partitions.
      numPartitions = Math.max(1, Math.round(Math.floor(Math.sqrt(approximateCount) / 10.0)));
    } else {
      // Fallback strategy: If no rows are estimated, or table stats are stale,
      // default to a fixed number of partitions to ensure it gets processed.
      numPartitions = 10; // A reasonable default
    }

    long totalRange = max - min;
    if (totalRange <= 0) {
      // If there's no data or only one element, create a single partition.
      c.output(KV.of(tableName, new Partition(min, max + 1)));
      return;
    }

    BigDecimal partitionSize = BigDecimal.valueOf(totalRange)
        .divide(BigDecimal.valueOf(numPartitions), RoundingMode.CEILING);

    if (partitionSize.compareTo(BigDecimal.ZERO) == 0) {
      partitionSize = BigDecimal.ONE;
    }

    long current = min;
    while (current <= max) {
      long end = current + partitionSize.longValue();
      c.output(KV.of(tableName, new Partition(current, end)));
      current = end;
    }
  }
}
