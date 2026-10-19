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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExportReadbackTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static SparkSession spark;

  @BeforeClass
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("ExportReadbackTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static final CollectionPlan USERS =
      CollectionPlan.create(
          new CollectionInfo("user-profiles", CollectionKind.DOCUMENT), "user_profiles", 1);

  private static final ExporterOptions OPTS =
      ExporterOptions.parse(new String[] {"--endpoints=h:1", "--output=/tmp/unused"});

  /** Writes rows through the exporter's own record mapping and Avro writer into staging. */
  static void writeBatch(SparkSession spark, CollectionPlan plan, List<Row> rows, String dir) {
    Dataset<Row> df = spark.createDataFrame(rows, plan.structType()).coalesce(1);
    CollectionExporter.writeAvro(plan.toBulkRecords(df), plan, OPTS, dir);
  }

  @Test
  public void countsFilesRowsAndBytes() throws Exception {
    File rootDir = tmp.newFolder("export root"); // space: exercises URL decoding
    // Hadoop URIs are not percent-decoded, so pass the raw path (with its space).
    String root = "file://" + rootDir.getAbsolutePath();
    String tableDir = CollectionExporter.tableDir(root, USERS);
    String stagingDir = CollectionExporter.stagingDir(root, USERS);
    String unicodeDoc = "{\"_key\":\"k3\",\"e\":\"😀é\"}";
    writeBatch(
        spark,
        USERS,
        Arrays.asList(
            RowFactory.create("k1", "c/k1", "_r1", "{\"_key\":\"k1\"}"),
            RowFactory.create("k2", null, null, "{\"_key\":\"k2\",\"n\":1}")),
        CollectionExporter.batchDir(stagingDir, 0));
    writeBatch(
        spark,
        USERS,
        Collections.singletonList(RowFactory.create("k3", "c/k3", "_r3", unicodeDoc)),
        CollectionExporter.batchDir(stagingDir, 1));
    CollectionExporter.commit(
        stagingDir, tableDir, USERS, spark.sparkContext().hadoopConfiguration());

    ExportReadback.Result r = ExportReadback.read(spark, tableDir, root);

    assertThat(r.rowCount).isEqualTo(3);
    assertThat(r.files).hasSize(2);
    long sum = 0;
    for (ExportSummary.FileEntry f : r.files) {
      assertThat(f.path)
          .matches("user_profiles/" + USERS.avroSchemaMd5() + "-0000[01]-of-00002.avro");
      assertWithMessage(f.path).that(new File(rootDir, f.path).isFile()).isTrue();
      sum += f.rows;
    }
    assertThat(sum).isEqualTo(3);
    assertThat(r.files.get(0).path)
        .isEqualTo("user_profiles/b36539a80425c7f2c6178856a7bb935f-00000-of-00002.avro");
    assertThat(r.files.get(0).rows).isEqualTo(2);
    assertThat(r.files.get(1).path)
        .isEqualTo("user_profiles/b36539a80425c7f2c6178856a7bb935f-00001-of-00002.avro");
    assertThat(r.files.get(1).rows).isEqualTo(1);
    int unicodeBytes = unicodeDoc.getBytes(StandardCharsets.UTF_8).length;
    assertThat(unicodeBytes).isGreaterThan(unicodeDoc.length());
    assertThat(r.maxDocBytes).isEqualTo(unicodeBytes);
    assertThat(r.oversizedKeys).isEmpty();
  }

  @Test
  public void listsOversizedKeys() throws Exception {
    String root = tmp.newFolder("big").toURI().toString().replaceAll("/$", "");
    CollectionPlan plan =
        CollectionPlan.create(new CollectionInfo("t", CollectionKind.DOCUMENT), "t", 1);
    String tableDir = CollectionExporter.tableDir(root, plan);
    String stagingDir = CollectionExporter.stagingDir(root, plan);
    StringBuilder sb = new StringBuilder("{\"x\":\"");
    while (sb.length() < ExportReadback.OVERSIZED_DOC_BYTES + 10) {
      sb.append("0123456789012345678901234567890123456789");
    }
    sb.append("\"}");
    writeBatch(
        spark,
        plan,
        Arrays.asList(
            RowFactory.create("big", null, null, sb.toString()),
            RowFactory.create("small", null, null, "{}")),
        CollectionExporter.batchDir(stagingDir, 0));
    CollectionExporter.commit(
        stagingDir, tableDir, plan, spark.sparkContext().hadoopConfiguration());
    ExportReadback.Result r = ExportReadback.read(spark, tableDir, root);
    assertThat(r.rowCount).isEqualTo(2);
    assertThat(r.maxDocBytes).isEqualTo(sb.length());
    assertThat(r.oversizedKeys).containsExactly("big");
  }

  @Test
  public void missingDirectoryIsEmpty() throws Exception {
    String root = tmp.newFolder("empty").toURI().toString();
    ExportReadback.Result r = ExportReadback.read(spark, root + "none", root);
    assertThat(r.rowCount).isEqualTo(0);
    assertThat(r.files).isEmpty();
  }

  @Test
  public void relativizeRejectsOutsideFiles() {
    Path root = new Path("gs://b/exports/e1");
    assertThat(ExportReadback.relativize(root, new Path("gs://b/exports/e1/t/p.avro")))
        .isEqualTo("t/p.avro");
    try {
      ExportReadback.relativize(root, new Path("gs://b/other/p.avro"));
      throw new AssertionError("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // ok
    }
  }
}
