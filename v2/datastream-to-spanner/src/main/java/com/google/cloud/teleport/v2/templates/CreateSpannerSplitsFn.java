package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.SplitPoints;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates pre-split points in a Spanner table based on a list of partition boundaries. This should
 * run to completion before any data is written to the table.
 */
public class CreateSpannerSplitsFn extends DoFn<String, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSpannerSplitsFn.class);

  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final PCollectionView<List<KV<String, Iterable<Partition>>>> allSampledPartitionsView;

  public CreateSpannerSplitsFn(String projectId, String instanceId, String databaseId,
      PCollectionView<List<KV<String, Iterable<Partition>>>> allSampledPartitionsView) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.allSampledPartitionsView = allSampledPartitionsView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    List<KV<String, Iterable<Partition>>> allSampledPartitions = c.sideInput(
        allSampledPartitionsView);
    for (KV<String, Iterable<Partition>> tablePartitions : allSampledPartitions) {
      String tableName = tablePartitions.getKey();
      Iterable<Partition> partitions = tablePartitions.getValue();
      List<SplitPoints> splitPointsList = new ArrayList<>();
      partitions.forEach(partition -> {
        // Don't create a split for the absolute minimum value.
        if (partition.lowerBound > 0) {
          ListValue key = ListValue.newBuilder()
              .addValues(
                  Value.newBuilder().setStringValue(String.valueOf(partition.lowerBound)).build())
              .build();
          SplitPoints splitPoint = SplitPoints.newBuilder()
              .setTable(tableName)
              .addKeys(SplitPoints.Key.newBuilder().setKeyParts(key).build()).build();
          splitPointsList.add(splitPoint);
        }

      });
      if (splitPointsList.isEmpty()) {
        return;
      }
      try (DatabaseAdminClient dbAdminClient = DatabaseAdminClient.create()) {
        DatabaseName dbName = DatabaseName.of(projectId, instanceId, databaseId);
        dbAdminClient.addSplitPoints(dbName, splitPointsList);
        LOG.info("Successfully created " + splitPointsList.size() + " split points for table: "
            + tableName);
        LOG.info("Sleeping for 10 secs before adding more");
        Thread.sleep(1000 * 10);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
