/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.tools.arangodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exports one collection: plans its AQL partition queries, groups them into batches of {@code
 * --rangesPerWrite}, and writes every batch as an independent Spark job (union of its partition
 * reads) to {@code tables/<table>/b<batch:05d>/}. Batches run concurrently on a shared write pool
 * of size {@code --parallelWrites}.
 */
public final class CollectionExporter {

  private static final Logger LOG = LoggerFactory.getLogger(CollectionExporter.class);

  static final String ARANGO_FORMAT = "com.arangodb.spark";

  private CollectionExporter() {}

  /** The table output directory: {@code <exportRoot>/tables/<table>}. */
  public static String tableDir(String exportRoot, CollectionPlan plan) {
    return exportRoot + "/tables/" + plan.tableName();
  }

  /**
   * The partition filter clauses for a collection: one empty filter when {@code plan.partitions()
   * == 1}, HASH filters for {@link PartitioningMode#HASH}, key-range filters for {@link
   * PartitioningMode#RANGE}. Turn them into queries with {@link AqlBuilder#exportQuery} or {@link
   * AqlBuilder#countQuery}.
   */
  public static List<String> planFilters(
      CollectionPlan plan, PartitioningMode mode, KeyRangePlanner.KeySource keys, long count) {
    int n = plan.partitions();
    List<String> filters = new ArrayList<>();
    if (n <= 1) {
      filters.add(AqlBuilder.rangeFilter(KeyRange.ALL));
    } else if (mode == PartitioningMode.HASH) {
      for (int i = 0; i < n; i++) {
        filters.add(AqlBuilder.hashFilter(plan.info(), n, i));
      }
    } else {
      for (KeyRange r : KeyRangePlanner.plan(keys, plan.sourceName(), count, n)) {
        filters.add(AqlBuilder.rangeFilter(r));
      }
    }
    return filters;
  }

  /** {@link #planFilters} turned into export queries. */
  public static List<String> planQueries(
      CollectionPlan plan, PartitioningMode mode, KeyRangePlanner.KeySource keys, long count) {
    List<String> queries = new ArrayList<>();
    for (String filter : planFilters(plan, mode, keys, count)) {
      queries.add(AqlBuilder.exportQuery(plan.info(), filter));
    }
    return queries;
  }

  /** Splits {@code items} into consecutive batches of at most {@code size}. */
  static <T> List<List<T>> batches(List<T> items, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("batch size must be >= 1");
    }
    List<List<T>> result = new ArrayList<>();
    for (int i = 0; i < items.size(); i += size) {
      result.add(
          Collections.unmodifiableList(
              new ArrayList<>(items.subList(i, Math.min(items.size(), i + size)))));
    }
    return result;
  }

  /**
   * Returns the single coordinator that partition query {@code queryIndex} is sent to.
   *
   * <p>A query-mode read is one Spark partition, and the connector's driver sends it to the first
   * configured endpoint (no load balancing), so passing the full list would funnel every export
   * query through one coordinator. Rotating a single endpoint per query spreads the gather and
   * {@code JSON_STRINGIFY} work over all coordinators.
   */
  static String endpointFor(Map<String, String> connOpts, int queryIndex) {
    String[] endpoints = connOpts.get("endpoints").split(",");
    return endpoints[Math.floorMod(queryIndex, endpoints.length)].trim();
  }

  /** Output directory of batch {@code index}. */
  static String batchDir(String tableDir, int index) {
    return String.format("%s/b%05d", tableDir, index);
  }

  /**
   * Writes all partition queries of a collection and returns the table output directory URI.
   *
   * @param writePool pool on which batch writes run (shared by all collections)
   */
  public static String export(
      SparkSession spark,
      CollectionPlan plan,
      List<String> queries,
      Map<String, String> connOpts,
      String exportRoot,
      ExporterOptions opts,
      ExecutorService writePool)
      throws Exception {
    String tableDir = tableDir(exportRoot, plan);
    List<List<String>> batches = batches(queries, opts.rangesPerWrite());
    LOG.info(
        "{}: {} partition queries in {} write batches -> {}",
        plan.sourceName(),
        queries.size(),
        batches.size(),
        tableDir);
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int b = 0; b < batches.size(); b++) {
      final int index = b;
      final List<String> batch = batches.get(b);
      tasks.add(
          () -> {
            writeBatch(spark, plan, batch, connOpts, batchDir(tableDir, index), opts, index);
            return null;
          });
    }
    // Fail fast: the first failing batch cancels every Spark job (the whole export fails anyway).
    runAllFailFast(writePool, tasks, () -> spark.sparkContext().cancelAllJobs());
    return tableDir;
  }

  /**
   * Runs {@code tasks} on {@code pool} and returns their results in submission order. Reacts to the
   * first failure in completion order (not submission order): cancels every other task, runs {@code
   * onFailure} and rethrows the failure's cause.
   */
  static <T> List<T> runAllFailFast(
      ExecutorService pool, List<? extends Callable<T>> tasks, Runnable onFailure)
      throws Exception {
    CompletionService<T> completion = new ExecutorCompletionService<>(pool);
    List<Future<T>> futures = new ArrayList<>();
    try {
      for (Callable<T> task : tasks) {
        futures.add(completion.submit(task));
      }
      for (int i = 0; i < futures.size(); i++) {
        completion.take().get();
      }
    } catch (ExecutionException | InterruptedException | RuntimeException e) {
      for (Future<T> f : futures) {
        f.cancel(true);
      }
      try {
        onFailure.run();
      } catch (RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        throw e;
      }
      Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
    List<T> results = new ArrayList<>();
    for (Future<T> f : futures) {
      results.add(f.get());
    }
    return results;
  }

  private static void writeBatch(
      SparkSession spark,
      CollectionPlan plan,
      List<String> queries,
      Map<String, String> connOpts,
      String outDir,
      ExporterOptions opts,
      int index) {
    spark
        .sparkContext()
        .setJobDescription(
            "arangodb-gcs-exporter " + plan.sourceName() + " batch " + index + " -> " + outDir);
    Dataset<Row> union = null;
    for (int i = 0; i < queries.size(); i++) {
      Dataset<Row> df =
          spark
              .read()
              .format(ARANGO_FORMAT)
              .options(connOpts)
              .option("endpoints", endpointFor(connOpts, index * opts.rangesPerWrite() + i))
              .option("query", queries.get(i))
              .schema(plan.structType())
              .load();
      union = union == null ? df : union.union(df);
    }
    long start = System.nanoTime();
    union
        .write()
        .format("avro")
        .option("avroSchema", plan.avroSchemaJson())
        .option("compression", opts.compression())
        .option("maxRecordsPerFile", Long.toString(opts.maxRecordsPerFile()))
        .mode(SaveMode.ErrorIfExists)
        .save(outDir);
    LOG.info(
        "{}: batch {} ({} queries) written to {} in {} s",
        plan.sourceName(),
        index,
        queries.size(),
        outDir,
        (System.nanoTime() - start) / 1_000_000_000L);
  }
}
