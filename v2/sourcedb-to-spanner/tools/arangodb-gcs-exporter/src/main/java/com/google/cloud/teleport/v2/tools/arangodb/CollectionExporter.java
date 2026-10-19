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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exports one collection: plans its AQL partition queries, groups them into batches of {@code
 * --rangesPerWrite}, and writes every batch as an independent Spark job (union of its partition
 * reads) to the staging directory {@code _staging/<table>/b<batch:05d>/}. Batches run concurrently
 * on a shared write pool of size {@code --parallelWrites}. When all batches are written, the part
 * files are renamed to {@code <table>/<md5Hex(schema)>-NNNNN-of-MMMMM.avro} (the naming of the
 * SourceDB to Spanner bulk GCS sink) and the table's staging directory is deleted.
 */
public final class CollectionExporter {

  private static final Logger LOG = LoggerFactory.getLogger(CollectionExporter.class);

  static final String ARANGO_FORMAT = "com.arangodb.spark";

  /** Staging directory below the export root; deleted when the export is complete. */
  static final String STAGING_DIR = "_staging";

  /** Upper bound of concurrent renames when committing a table's files. */
  static final int MAX_PARALLEL_RENAMES = 32;

  private CollectionExporter() {}

  /** The table output directory: {@code <exportRoot>/<table>}. */
  public static String tableDir(String exportRoot, CollectionPlan plan) {
    return exportRoot + "/" + plan.tableName();
  }

  /** The staging root: {@code <exportRoot>/_staging}. */
  public static String stagingRoot(String exportRoot) {
    return exportRoot + "/" + STAGING_DIR;
  }

  /** The table staging directory: {@code <exportRoot>/_staging/<table>}. */
  public static String stagingDir(String exportRoot, CollectionPlan plan) {
    return stagingRoot(exportRoot) + "/" + plan.tableName();
  }

  /**
   * Final name of data file {@code index} of {@code total}: {@code
   * <md5>-<index:05d>-of-<total:05d>.avro}, as the bulk sink's Beam default naming produces.
   */
  static String bulkFileName(String schemaMd5, int index, int total) {
    return String.format("%s-%05d-of-%05d.avro", schemaMd5, index, total);
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
   * Writes all partition queries of a collection to staging, commits the files into the table
   * directory with bulk names and returns the table output directory URI.
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
    String stagingDir = stagingDir(exportRoot, plan);
    List<List<String>> batches = batches(queries, opts.rangesPerWrite());
    LOG.info(
        "{}: {} partition queries in {} write batches -> {} (staging {})",
        plan.sourceName(),
        queries.size(),
        batches.size(),
        tableDir,
        stagingDir);
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int b = 0; b < batches.size(); b++) {
      final int index = b;
      final List<String> batch = batches.get(b);
      tasks.add(
          () -> {
            writeBatch(spark, plan, batch, connOpts, batchDir(stagingDir, index), opts, index);
            return null;
          });
    }
    // Fail fast: the first failing batch cancels every Spark job (the whole export fails anyway).
    runAllFailFast(writePool, tasks, () -> spark.sparkContext().cancelAllJobs());
    commit(stagingDir, tableDir, plan, spark.sparkContext().hadoopConfiguration());
    return tableDir;
  }

  /**
   * Moves every staged part file ({@code *.avro} below {@code stagingDir}, hidden files skipped)
   * into {@code tableDir} as {@code <md5>-NNNNN-of-MMMMM.avro}, in sorted staging path order, then
   * deletes {@code stagingDir} (with Spark's {@code _SUCCESS} markers). Renames run in parallel.
   *
   * @return the final file paths, in index order
   * @throws IllegalStateException if a destination exists or a rename fails
   */
  static List<Path> commit(
      String stagingDir, String tableDir, CollectionPlan plan, Configuration conf)
      throws Exception {
    List<Path> staged = new ArrayList<>(HadoopFiles.listFiles(stagingDir, ".avro", conf));
    Collections.sort(staged);
    Path target = new Path(tableDir);
    FileSystem fs = target.getFileSystem(conf);
    // On file:// the checksummed LocalFileSystem would also move the hidden .crc files into the
    // table directory; renaming with the raw file system keeps only the data files there.
    FileSystem renameFs =
        fs instanceof ChecksumFileSystem ? ((ChecksumFileSystem) fs).getRawFileSystem() : fs;
    if (!staged.isEmpty() && !fs.mkdirs(target)) {
      throw new IllegalStateException("Cannot create " + tableDir);
    }
    List<Path> finals = new ArrayList<>();
    List<Callable<Void>> renames = new ArrayList<>();
    for (int i = 0; i < staged.size(); i++) {
      Path src = staged.get(i);
      Path dst =
          fs.makeQualified(new Path(target, bulkFileName(plan.avroSchemaMd5(), i, staged.size())));
      finals.add(dst);
      renames.add(
          () -> {
            if (fs.exists(dst)) {
              throw new IllegalStateException("Destination " + dst + " already exists");
            }
            if (!renameFs.rename(src, dst)) {
              throw new IllegalStateException("Cannot rename " + src + " to " + dst);
            }
            return null;
          });
    }
    if (!renames.isEmpty()) {
      ExecutorService pool =
          Executors.newFixedThreadPool(
              Math.min(MAX_PARALLEL_RENAMES, renames.size()),
              r -> {
                Thread t = new Thread(r, "arangodb-gcs-exporter-rename");
                t.setDaemon(true);
                return t;
              });
      try {
        runAllFailFast(pool, renames, () -> {});
      } finally {
        pool.shutdownNow();
      }
    }
    Path staging = new Path(stagingDir);
    FileSystem stagingFs = staging.getFileSystem(conf);
    if (stagingFs.exists(staging) && !stagingFs.delete(staging, true)) {
      throw new IllegalStateException("Cannot delete staging directory " + stagingDir);
    }
    LOG.info("{}: committed {} files to {}", plan.sourceName(), finals.size(), tableDir);
    return finals;
  }

  /** Deletes {@code <exportRoot>/_staging} if it exists (it is empty after all commits). */
  static void deleteStagingRoot(String exportRoot, Configuration conf) throws IOException {
    Path staging = new Path(stagingRoot(exportRoot));
    FileSystem fs = staging.getFileSystem(conf);
    if (fs.exists(staging) && !fs.delete(staging, true)) {
      throw new IOException("Cannot delete " + staging);
    }
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
    writeAvro(plan.toBulkRecords(union), plan, opts, outDir);
    LOG.info(
        "{}: batch {} ({} queries) written to {} in {} s",
        plan.sourceName(),
        index,
        queries.size(),
        outDir,
        (System.nanoTime() - start) / 1_000_000_000L);
  }

  /**
   * Writes bulk records ({@link CollectionPlan#toBulkRecords}) to {@code outDir} with spark-avro
   * and the plan's writer schema, so the file header schema is exactly {@link
   * CollectionPlan#avroSchemaJson()}.
   */
  static void writeAvro(
      Dataset<Row> records, CollectionPlan plan, ExporterOptions opts, String outDir) {
    records
        .write()
        .format("avro")
        .option("avroSchema", plan.avroSchemaJson())
        .option("compression", opts.compression())
        .option("maxRecordsPerFile", Long.toString(opts.maxRecordsPerFile()))
        .mode(SaveMode.ErrorIfExists)
        .save(outDir);
  }
}
