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
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark job that exports ArangoDB collections to {@code <output>/<exportId>/} in the SourceDB to
 * Spanner bulk GCS format ({@code <table>/<md5Hex(schema)>-NNNNN-of-MMMMM.avro}), plus suggested
 * Spanner DDL and an {@code arangodb-export.json} summary (written last, the completion marker).
 *
 * <p>Exit codes: 0 success, 1 invalid options, 2 inconsistent export (source changed or counts
 * differ; no summary written), 3 any other failure.
 */
public final class ArangoDbToGcsExporter {

  private static final Logger LOG = LoggerFactory.getLogger(ArangoDbToGcsExporter.class);

  static final String APP_NAME = "arangodb-gcs-exporter";

  private ArangoDbToGcsExporter() {}

  /** Entry point for {@code spark-submit}. */
  public static void main(String[] args) {
    int code;
    SparkSession spark = null;
    try {
      ExporterOptions opts = ExporterOptions.parse(args);
      LOG.info("Starting {} with {}", APP_NAME, opts);
      spark = SparkSession.builder().appName(APP_NAME).getOrCreate();
      run(opts, spark);
      code = 0;
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid options or input: {}", e.getMessage(), e);
      code = 1;
    } catch (ExportException e) {
      LOG.error("Export is not consistent; no summary was written: {}", e.getMessage());
      code = 2;
    } catch (Throwable t) {
      LOG.error("Export failed", t);
      code = 3;
    } finally {
      if (spark != null) {
        spark.stop();
      }
    }
    System.exit(code);
  }

  /**
   * Runs the export and returns the written summary, or {@code null} with {@code --planOnly}.
   *
   * @throws ExportException if the source changed during the export or counts differ (and {@code
   *     --allowLiveSource} is false)
   * @throws IllegalArgumentException on bad options (unknown collections, existing export, ...)
   */
  public static ExportSummary run(ExporterOptions opts, SparkSession spark) throws Exception {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    String password = PasswordResolver.resolve(opts, conf);
    try (ArangoMetadataClient client = new ArangoMetadataClient(opts, password)) {
      return run(opts, spark, client, opts.connectorBaseOptions(password));
    }
  }

  static ExportSummary run(
      ExporterOptions opts,
      SparkSession spark,
      ArangoMetadataClient client,
      Map<String, String> connOpts)
      throws Exception {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    String exportRoot = opts.exportRoot();

    String serverVersion = client.serverVersion();
    List<CollectionInfo> selected =
        selectCollections(client.listCollections(), opts.collections(), opts.excludeCollections());
    if (selected.isEmpty()) {
      throw new IllegalArgumentException("No collections selected for export");
    }
    List<String> names = new ArrayList<>();
    for (CollectionInfo c : selected) {
      AqlBuilder.quoteCollection(c.sourceName()); // fail fast on unsupported names
      names.add(c.sourceName());
    }
    Map<String, String> tableNames = NameSanitizer.toTableNames(names);

    if (!opts.planOnly()) {
      checkNoExistingExport(exportRoot, conf);
    }

    Instant startedAt = Instant.now();
    LOG.info(
        "Exporting {} collections from ArangoDB {} database '{}' to {} (partitioning={})",
        selected.size(),
        serverVersion,
        opts.database(),
        exportRoot,
        opts.partitioning());

    ExecutorService collectionPool =
        Executors.newFixedThreadPool(opts.parallelCollections(), daemonThreads("collection"));
    ExecutorService writePool =
        Executors.newFixedThreadPool(opts.parallelWrites(), daemonThreads("write"));
    List<CollectionResult> results = new ArrayList<>();
    try {
      List<Callable<CollectionResult>> tasks = new ArrayList<>();
      for (CollectionInfo info : selected) {
        String table = tableNames.get(info.sourceName());
        tasks.add(
            () ->
                exportCollection(
                    spark, client, info, table, connOpts, exportRoot, opts, writePool));
      }
      // Fail fast on the first failing collection, whatever its position in the list.
      results.addAll(
          CollectionExporter.runAllFailFast(
              collectionPool, tasks, () -> spark.sparkContext().cancelAllJobs()));
    } finally {
      collectionPool.shutdownNow();
      writePool.shutdownNow();
    }
    Instant finishedAt = Instant.now();

    if (opts.planOnly()) {
      for (CollectionResult r : results) {
        LOG.info(
            "Plan for {}: {} partitions (requested {}), source count {}",
            r.plan.sourceName(),
            r.partitionCount,
            r.plan.partitions(),
            r.countBefore);
      }
      LOG.info("--planOnly=true: nothing was exported");
      return null;
    }

    CollectionExporter.deleteStagingRoot(exportRoot, conf);

    ExportSummary summary = new ExportSummary();
    summary.exportId = opts.exportId();
    summary.producer = producer(spark.version());
    summary.source = new ExportSummary.Source();
    summary.source.system = "arangodb";
    summary.source.database = opts.database();
    summary.source.serverVersion = serverVersion;
    summary.source.exportStartedAt = startedAt.toString();
    summary.source.exportFinishedAt = finishedAt.toString();
    List<CollectionPlan> plans = new ArrayList<>();
    for (CollectionResult r : results) {
      summary.tables.add(r.toTable());
      plans.add(r.plan);
    }

    List<String> problems = consistencyProblems(summary.tables);
    if (!problems.isEmpty()) {
      if (!opts.allowLiveSource()) {
        throw new ExportException(
            String.join("; ", problems)
                + ". Quiesce the source and re-run with a new --exportId, or pass"
                + " --allowLiveSource=true to accept an inconsistent export.");
      }
      LOG.warn("Source changed during export (allowed by --allowLiveSource): {}", problems);
      summary.source.consistent = false;
    } else {
      summary.source.consistent = true;
    }

    HadoopFiles.writeString(
        exportRoot + "/" + SpannerDdlGenerator.GOOGLESQL_FILE,
        SpannerDdlGenerator.googleSql(plans, opts.ddlEdgeHelpers()),
        conf,
        true);
    HadoopFiles.writeString(
        exportRoot + "/" + SpannerDdlGenerator.POSTGRESQL_FILE,
        SpannerDdlGenerator.postgreSql(plans),
        conf,
        true);
    ExportSummaryWriter.write(summary, exportRoot, conf);

    for (ExportSummary.Table t : summary.tables) {
      LOG.info(
          "Table {} (collection {}, {}): {} rows in {} files, max doc {} bytes, {} oversized",
          t.name,
          t.sourceName,
          t.collectionType,
          t.rowCount,
          t.files.size(),
          t.stats.maxDocBytes,
          t.stats.oversizedKeys.size());
    }
    LOG.info(
        "Export complete: {} (summary {}, consistent={})",
        exportRoot,
        ExportSummaryWriter.SUMMARY_FILE,
        summary.source.consistent);
    return summary;
  }

  /**
   * Refuses to write into an export root that already has a summary or any data from an earlier
   * run: the bulk layout puts table directories directly below the root, so the root must be
   * missing or empty.
   *
   * @throws IllegalArgumentException if {@code arangodb-export.json} or any other entry exists
   */
  static void checkNoExistingExport(String exportRoot, Configuration conf) {
    if (ExportSummaryWriter.exists(exportRoot, conf)) {
      throw new IllegalArgumentException(
          "Export "
              + exportRoot
              + " already has an "
              + ExportSummaryWriter.SUMMARY_FILE
              + "; choose a new --exportId");
    }
    if (!HadoopFiles.isEmptyOrMissing(exportRoot, conf)) {
      throw new IllegalArgumentException(
          "Export "
              + exportRoot
              + " contains data from an earlier incomplete run; delete it or choose a new"
              + " --exportId");
    }
  }

  private static CollectionResult exportCollection(
      SparkSession spark,
      ArangoMetadataClient client,
      CollectionInfo info,
      String table,
      Map<String, String> connOpts,
      String exportRoot,
      ExporterOptions opts,
      ExecutorService writePool)
      throws Exception {
    String name = info.sourceName();
    long countBefore = client.count(name);
    String revisionBefore = client.revision(name);
    CollectionPlan plan = CollectionPlan.create(info, table, countBefore, opts);
    List<String> filters =
        CollectionExporter.planFilters(plan, opts.partitioning(), client, countBefore);
    List<String> queries = new ArrayList<>();
    for (String filter : filters) {
      queries.add(AqlBuilder.exportQuery(info, filter));
    }
    if (opts.partitioning() == PartitioningMode.RANGE && opts.verifyRangePlans()) {
      RangePlanChecker.check(name, queries, client::explainNodes);
    }
    if (opts.verifyPartitions()) {
      verifyPartitions(client::countQuery, plan, filters, countBefore, opts.allowLiveSource());
    }
    if (opts.planOnly()) {
      return new CollectionResult(
          plan, filters.size(), countBefore, countBefore, revisionBefore, revisionBefore, null);
    }
    String tableDir =
        CollectionExporter.export(spark, plan, queries, connOpts, exportRoot, opts, writePool);
    ExportReadback.Result readback = ExportReadback.read(spark, tableDir, exportRoot);
    long countAfter = client.count(name);
    String revisionAfter = client.revision(name);
    LOG.info(
        "{}: exported {} rows (source count before {}, after {}) with {} queries",
        name,
        readback.rowCount,
        countBefore,
        countAfter,
        queries.size());
    return new CollectionResult(
        plan, queries.size(), countBefore, countAfter, revisionBefore, revisionAfter, readback);
  }

  /**
   * Counts every partition with the same FILTER text as its export query and requires the sum to
   * equal the collection count, i.e. that partitions are disjoint and complete.
   */
  static void verifyPartitions(
      ToLongFunction<String> countQuery,
      CollectionPlan plan,
      List<String> filters,
      long expected,
      boolean allowLiveSource)
      throws ExportException {
    long sum = 0;
    long emptyPartitions = 0;
    for (String filter : filters) {
      long n = countQuery.applyAsLong(AqlBuilder.countQuery(plan.info(), filter));
      if (n == 0) {
        emptyPartitions++;
      }
      sum += n;
    }
    String msg =
        String.format(
            "%s: %d partitions count %d documents in total (%d empty); collection count is %d",
            plan.sourceName(), filters.size(), sum, emptyPartitions, expected);
    if (sum == expected) {
      LOG.info("Partition verification OK. {}", msg);
      return;
    }
    if (allowLiveSource) {
      LOG.warn("Partition verification mismatch (allowed by --allowLiveSource). {}", msg);
      return;
    }
    throw new ExportException("Partition verification failed. " + msg);
  }

  /**
   * Applies {@code --collections} (order preserved; every name must exist) and {@code
   * --excludeCollections}; system collections ({@code _*}) are always dropped.
   */
  static List<CollectionInfo> selectCollections(
      List<CollectionInfo> available, List<String> include, List<String> exclude) {
    Map<String, CollectionInfo> byName = new LinkedHashMap<>();
    for (CollectionInfo c : available) {
      if (!c.sourceName().startsWith("_")) {
        byName.put(c.sourceName(), c);
      }
    }
    List<CollectionInfo> result = new ArrayList<>();
    if (include.isEmpty()) {
      result.addAll(byName.values());
    } else {
      Set<String> unknown = new LinkedHashSet<>();
      for (String n : new LinkedHashSet<>(include)) {
        CollectionInfo c = byName.get(n);
        if (c == null) {
          unknown.add(n);
        } else {
          result.add(c);
        }
      }
      if (!unknown.isEmpty()) {
        throw new IllegalArgumentException(
            "Collections not found (or system collections): " + unknown);
      }
    }
    Set<String> excluded = new LinkedHashSet<>(exclude);
    result.removeIf(c -> excluded.contains(c.sourceName()));
    return result;
  }

  /** D4: per table, the count and revision must not change and rows must equal countBefore. */
  static List<String> consistencyProblems(List<ExportSummary.Table> tables) {
    List<String> problems = new ArrayList<>();
    for (ExportSummary.Table t : tables) {
      if (!t.sourceCountBefore.equals(t.sourceCountAfter)) {
        problems.add(
            t.sourceName
                + ": document count changed from "
                + t.sourceCountBefore
                + " to "
                + t.sourceCountAfter);
      }
      if (t.sourceRevisionBefore != null && !t.sourceRevisionBefore.equals(t.sourceRevisionAfter)) {
        problems.add(
            t.sourceName
                + ": revision changed from "
                + t.sourceRevisionBefore
                + " to "
                + t.sourceRevisionAfter);
      }
      if (!t.rowCount.equals(t.sourceCountBefore)) {
        problems.add(
            t.sourceName
                + ": exported "
                + t.rowCount
                + " rows but the collection had "
                + t.sourceCountBefore
                + " documents");
      }
    }
    return problems;
  }

  static String producer(String sparkVersion) {
    Properties p = new Properties();
    try (InputStream in =
        ArangoDbToGcsExporter.class.getResourceAsStream("/arangodb-gcs-exporter.properties")) {
      if (in != null) {
        p.load(in);
      }
    } catch (IOException e) {
      LOG.debug("Cannot read version properties", e);
    }
    return APP_NAME
        + "/"
        + p.getProperty("exporter.version", "unknown")
        + " (spark "
        + sparkVersion
        + ", connector "
        + p.getProperty("connector.version", "unknown")
        + ")";
  }

  private static ThreadFactory daemonThreads(String prefix) {
    AtomicInteger n = new AtomicInteger();
    return r -> {
      Thread t = new Thread(r, APP_NAME + "-" + prefix + "-" + n.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  /** Everything learned about one exported collection. */
  static final class CollectionResult {
    final CollectionPlan plan;
    final int partitionCount;
    final long countBefore;
    final long countAfter;
    final String revisionBefore;
    final String revisionAfter;
    final ExportReadback.Result readback;

    CollectionResult(
        CollectionPlan plan,
        int partitionCount,
        long countBefore,
        long countAfter,
        String revisionBefore,
        String revisionAfter,
        ExportReadback.Result readback) {
      this.plan = plan;
      this.partitionCount = partitionCount;
      this.countBefore = countBefore;
      this.countAfter = countAfter;
      this.revisionBefore = revisionBefore;
      this.revisionAfter = revisionAfter;
      this.readback = readback;
    }

    ExportSummary.Table toTable() {
      ExportSummary.Table t = new ExportSummary.Table();
      t.name = plan.tableName();
      t.sourceName = plan.sourceName();
      t.collectionType = plan.kind().name();
      t.primaryKey = new ArrayList<>(Collections.singletonList("key"));
      for (ExportSummary.Column c : plan.columns()) {
        t.columns.add(new ExportSummary.Column(c.name, c.type, c.nullable, c.sourceName));
      }
      t.rowCount = readback.rowCount;
      t.sourceCountBefore = countBefore;
      t.sourceCountAfter = countAfter;
      t.sourceRevisionBefore = revisionBefore;
      t.sourceRevisionAfter = revisionAfter;
      t.files = new ArrayList<>(readback.files);
      t.stats = new ExportSummary.Stats(readback.maxDocBytes, readback.oversizedKeys);
      return t;
    }
  }
}
