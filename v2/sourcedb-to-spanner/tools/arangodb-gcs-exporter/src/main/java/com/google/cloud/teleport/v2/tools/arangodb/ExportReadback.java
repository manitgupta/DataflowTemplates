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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.slice;
import static org.apache.spark.sql.functions.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads the written Avro files back and computes the ground truth for the manifest: rows per file,
 * total rows, the largest {@code doc} in bytes and the keys of oversized documents.
 */
public final class ExportReadback {

  /** Documents above this size (bytes) are listed in {@code stats.oversizedKeys}. */
  public static final long OVERSIZED_DOC_BYTES = 10_000_000L;

  static final int MAX_OVERSIZED_KEYS = 100;

  private ExportReadback() {}

  /** Readback result for one table. */
  public static final class Result {
    public final long rowCount;
    public final List<ExportManifest.FileEntry> files;
    public final long maxDocBytes;
    public final List<String> oversizedKeys;

    Result(
        long rowCount,
        List<ExportManifest.FileEntry> files,
        long maxDocBytes,
        List<String> oversizedKeys) {
      this.rowCount = rowCount;
      this.files = files;
      this.maxDocBytes = maxDocBytes;
      this.oversizedKeys = oversizedKeys;
    }
  }

  /**
   * Reads every Avro file under {@code tableDir} (recursively) and returns per-file counts with
   * paths relative to {@code exportRoot}. Files that exist but contain no rows are listed with
   * {@code rows = 0}.
   */
  public static Result read(SparkSession spark, String tableDir, String exportRoot) {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    List<Path> listed = HadoopFiles.listFiles(tableDir, ".avro", conf);
    if (listed.isEmpty()) {
      return new Result(0, new ArrayList<>(), 0, new ArrayList<>());
    }
    Path root = HadoopFiles.qualify(exportRoot, conf);
    // Qualified path string -> rows; starts with every listed file at 0.
    Map<String, Long> rowsByFile = new TreeMap<>();
    for (Path p : listed) {
      rowsByFile.put(p.toString(), 0L);
    }

    Dataset<Row> df =
        spark
            .read()
            .format("avro")
            .option("recursiveFileLookup", "true")
            .load(tableDir)
            .select(col("key"), expr("octet_length(doc)").as("len"), input_file_name().as("f"));
    // One pass over the data: oversized keys are collected in the same aggregation (collect_list
    // skips the nulls of non-oversized rows; a file's list is tiny relative to its >10 MB docs).
    List<Row> perFile =
        df.groupBy(col("f"))
            .agg(
                count(lit(1)).as("n"),
                max(col("len")).as("m"),
                slice(
                        collect_list(when(col("len").gt(OVERSIZED_DOC_BYTES), col("key"))),
                        1,
                        MAX_OVERSIZED_KEYS)
                    .as("o"))
            .collectAsList();

    long total = 0;
    long maxDocBytes = 0;
    List<String> oversized = new ArrayList<>();
    Map<String, FileSystem> fsCache = new HashMap<>();
    for (Row r : perFile) {
      if (!r.isNullAt(3)) {
        oversized.addAll(r.<String>getList(3));
      }
      String qualified = qualifyInputFile(r.getString(0), conf, fsCache);
      long n = r.getLong(1);
      long m = r.isNullAt(2) ? 0 : ((Number) r.get(2)).longValue();
      if (!rowsByFile.containsKey(qualified)) {
        throw new IllegalStateException(
            "Readback found file " + qualified + " that was not listed under " + tableDir);
      }
      rowsByFile.put(qualified, n);
      total += n;
      maxDocBytes = Math.max(maxDocBytes, m);
    }

    List<ExportManifest.FileEntry> files = new ArrayList<>();
    for (Map.Entry<String, Long> e : rowsByFile.entrySet()) {
      files.add(new ExportManifest.FileEntry(relativize(root, new Path(e.getKey())), e.getValue()));
    }

    Collections.sort(oversized);
    if (oversized.size() > MAX_OVERSIZED_KEYS) {
      oversized = new ArrayList<>(oversized.subList(0, MAX_OVERSIZED_KEYS));
    }
    return new Result(total, files, maxDocBytes, oversized);
  }

  /** Turns Spark's URL-encoded {@code input_file_name()} into a qualified Hadoop path string. */
  static String qualifyInputFile(
      String inputFileName, Configuration conf, Map<String, FileSystem> fsCache) {
    try {
      Path p = new Path(new URI(inputFileName));
      String key = p.toUri().getScheme() + "://" + p.toUri().getAuthority();
      FileSystem fs = fsCache.get(key);
      if (fs == null) {
        fs = p.getFileSystem(conf);
        fsCache.put(key, fs);
      }
      return fs.makeQualified(p).toString();
    } catch (URISyntaxException | java.io.IOException e) {
      throw new IllegalStateException("Cannot parse input file name " + inputFileName, e);
    }
  }

  /** Path of {@code file} relative to {@code root}; fails if {@code file} is not below it. */
  static String relativize(Path root, Path file) {
    URI rel = root.toUri().relativize(file.toUri());
    if (rel.isAbsolute()
        || rel.getPath().startsWith("/")
        || rel.getPath().equals(file.toString())) {
      throw new IllegalStateException("File " + file + " is not below export root " + root);
    }
    return rel.getPath();
  }
}
