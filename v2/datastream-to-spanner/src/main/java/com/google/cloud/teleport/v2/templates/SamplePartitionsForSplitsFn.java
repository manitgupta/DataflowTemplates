package com.google.cloud.teleport.v2.templates;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;


/**
 * Samples a list of partitions to a maximum of 100 for creating Spanner splits.
 * It selects partitions at an even interval to ensure they remain representative.
 */
public class SamplePartitionsForSplitsFn extends DoFn<KV<String, Iterable<Partition>>, KV<String, Iterable<Partition>>> {

  private static final int MAX_SPLIT_POINTS = 100;

  @ProcessElement
  public void processElement(ProcessContext c) {
    String tableName = c.element().getKey();
    List<Partition> allPartitions = Lists.newArrayList(c.element().getValue());

    if (allPartitions.size() <= MAX_SPLIT_POINTS) {
      // If we are already under the limit, just pass them through.
      c.output(KV.of(tableName, allPartitions));
      return;
    }

    // --- Uniform Sampling Algorithm ---
    List<Partition> sampledPartitions = new ArrayList<>();
    double step = (double) allPartitions.size() / MAX_SPLIT_POINTS;

    for (int i = 0; i < MAX_SPLIT_POINTS; i++) {
      int index = (int) Math.round(i * step);
      // Ensure index is within bounds
      index = Math.min(index, allPartitions.size() - 1);
      sampledPartitions.add(allPartitions.get(index));
    }

    c.output(KV.of(tableName, sampledPartitions));
  }
}
