package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.SplitPoints;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates pre-split points in a Spanner table based on a list of partition boundaries.
 * This should run to completion before any data is written to the table.
 */
public class CreateSpannerSplitsFn extends DoFn<KV<String, Iterable<Partition>>, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSpannerSplitsFn.class);

  private final String projectId;
  private final String instanceId;
  private final String databaseId;

  public CreateSpannerSplitsFn(String projectId, String instanceId, String databaseId) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    String tableName = c.element().getKey();
    Iterable<Partition> partitions = c.element().getValue();

    // The DatabaseAdminClient is used for schema modifications like adding splits.
    try (DatabaseAdminClient dbAdminClient = DatabaseAdminClient.create()) {
      DatabaseName dbName = DatabaseName.of(projectId, instanceId, databaseId);

      // Convert our Partition objects into Spanner's SplitPoints protobuf format.
      // We use the 'lowerBound' of each partition as the split point, skipping the first.
      List<SplitPoints.Builder> splitPointBuilders = new ArrayList<>();
      partitions.forEach(partition -> {
        // Don't create a split for the absolute minimum value.
        if (partition.lowerBound > 0) {
          // The key for the split point. For a single primary key, this is a list with one value.
          ListValue key = ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(String.valueOf(partition.lowerBound)).build())
              .build();

          splitPointBuilders.add(SplitPoints.newBuilder().setTable(tableName)
                  .addKeys(SplitPoints.Key.newBuilder().setKeyParts(key).build()));
        }
      });

      if (splitPointBuilders.isEmpty()) {
        // No splits to create for this table.
        return;
      }

      List<SplitPoints> splitPointsList = splitPointBuilders.stream()
          .map(SplitPoints.Builder::build)
          .collect(Collectors.toList());

      // The API call to add the split points to the specified table.
      dbAdminClient.addSplitPoints(dbName, splitPointsList);
      LOG.info("Successfully created " + splitPointsList.size() + " split points for table: " + tableName);
    }
  }
}
