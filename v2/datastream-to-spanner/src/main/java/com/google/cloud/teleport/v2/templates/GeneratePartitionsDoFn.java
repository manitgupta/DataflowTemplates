package com.google.cloud.teleport.v2.templates;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate partitions.
 */
public class GeneratePartitionsDoFn extends DoFn<KV<String, String>, KV<String, Partition>> {

  private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private transient DataSource dataSource;

  private static final Logger LOG = LoggerFactory.getLogger(GeneratePartitionsDoFn.class);

  public GeneratePartitionsDoFn(JdbcIO.DataSourceConfiguration dataSourceConfiguration) {
    this.dataSourceConfiguration = dataSourceConfiguration;
  }

  @Setup
  public void setup() {
    // Each worker gets its own DataSource pool
    dataSource = dataSourceConfiguration.buildDatasource();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    KV<String, String> element = c.element();
    String tableName = element.getKey();
    String partitionColumn = element.getValue();

    long min = 0;
    long max = 0;
    long count = 0;

    // Step 1: Query for min, max, and count to determine partition ranges
    String query = String.format("SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s",
        partitionColumn, partitionColumn, tableName);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery()) {

      if (rs.next()) {
        min = rs.getLong(1);
        max = rs.getLong(2);
        count = rs.getLong(3);
      }
    }

    if (count == 0) {
      // No records, no partitions to generate
      return;
    }

    // Step 2: Calculate number of partitions using the same logic as JdbcIO
    // We take the square root of the number of rows, and divide it by 10
    long numPartitions = Math.max(1, Math.round(Math.floor(Math.sqrt(count) / 10.0)));
    LOG.info("tableName = {}, numPartitions = {}", tableName, numPartitions);
    // Step 3: Generate the actual partition objects
    BigDecimal partitionSize = BigDecimal.valueOf(max)
        .subtract(BigDecimal.valueOf(min))
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
