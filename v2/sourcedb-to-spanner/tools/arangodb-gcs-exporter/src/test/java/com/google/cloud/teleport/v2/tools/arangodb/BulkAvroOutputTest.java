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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verifies with Avro's own {@link DataFileReader} that the exporter writes the SourceDB to Spanner
 * bulk GCS format: the file header schema is byte for byte the golden schema (including {@code
 * logicalType: json}), files are named {@code <md5Hex(schema)>-NNNNN-of-MMMMM.avro} with nothing
 * else in the table directory, and every record carries the right values.
 */
@RunWith(JUnit4.class)
public class BulkAvroOutputTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static SparkSession spark;

  @BeforeClass
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("BulkAvroOutputTest")
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

  private static String golden(String name) throws Exception {
    return new String(
        Files.readAllBytes(Paths.get("src/main/resources/avro", name)), StandardCharsets.UTF_8);
  }

  /** Reads every record of every file in name order; checks each file header on the way. */
  private static Map<String, GenericRecord> readAll(
      File tableDir, List<String> names, String goldenSchema) throws Exception {
    Map<String, GenericRecord> byKey = new TreeMap<>();
    for (String name : names) {
      try (DataFileReader<GenericRecord> reader =
          new DataFileReader<>(new File(tableDir, name), new GenericDatumReader<>())) {
        // The literal header text and the parsed schema both equal the golden schema.
        assertThat(reader.getMetaString("avro.schema")).isEqualTo(goldenSchema);
        assertThat(reader.getSchema().toString()).isEqualTo(goldenSchema);
        Schema doc = reader.getSchema().getField("payload").schema().getField("doc").schema();
        assertThat(doc.getTypes().get(1).getProp("logicalType")).isEqualTo("json");
        assertThat(reader.getMetaString("avro.codec")).isEqualTo("snappy");
        for (GenericRecord r : reader) {
          GenericRecord payload = (GenericRecord) r.get("payload");
          byKey.put(payload.get("key").toString(), r);
        }
      }
    }
    return byKey;
  }

  private static List<String> sortedNames(File dir) {
    // File.list() includes hidden files (.crc, _SUCCESS): there must be none.
    List<String> names = new ArrayList<>(Arrays.asList(dir.list()));
    Collections.sort(names);
    return names;
  }

  private static String str(Object o) {
    return o == null ? null : o.toString();
  }

  private static void assertEnvelope(GenericRecord r, String table) {
    assertThat(str(r.get("tableName"))).isEqualTo(table);
    assertThat(r.get("shardId")).isNull();
    List<String> pks = new ArrayList<>();
    for (Object o : (List<?>) r.get("primaryKeys")) {
      pks.add(o.toString());
    }
    assertThat(pks).containsExactly("key");
  }

  @Test
  public void documentCollection() throws Exception {
    String root = "file://" + tmp.newFolder("export").getAbsolutePath();
    CollectionPlan plan =
        CollectionPlan.create(
            new CollectionInfo("user-profiles", CollectionKind.DOCUMENT), "user_profiles", 1);
    String stagingDir = CollectionExporter.stagingDir(root, plan);
    String tableDir = CollectionExporter.tableDir(root, plan);
    String unicodeDoc = "{\"_key\":\"k3\",\"e\":\"😀 café\",\"n\":1.50}";
    ExportReadbackTest.writeBatch(
        spark,
        plan,
        Arrays.asList(
            RowFactory.create("k1", "user-profiles/k1", "_r1", "{\"_key\":\"k1\"}"),
            RowFactory.create("k2", null, null, "{\"_key\":\"k2\",\"a\":[1,null]}")),
        CollectionExporter.batchDir(stagingDir, 0));
    ExportReadbackTest.writeBatch(
        spark,
        plan,
        Collections.singletonList(RowFactory.create("k3", "user-profiles/k3", "_r3", unicodeDoc)),
        CollectionExporter.batchDir(stagingDir, 1));

    List<Path> committed =
        CollectionExporter.commit(
            stagingDir, tableDir, plan, spark.sparkContext().hadoopConfiguration());

    File dir = new File(new Path(tableDir).toUri().getPath());
    String md5 = "b36539a80425c7f2c6178856a7bb935f";
    assertThat(plan.avroSchemaMd5()).isEqualTo(md5);
    List<String> names = sortedNames(dir);
    assertThat(names).containsExactly(md5 + "-00000-of-00002.avro", md5 + "-00001-of-00002.avro");
    assertThat(committed).hasSize(2);
    assertThat(committed.get(0).getName()).isEqualTo(names.get(0));
    assertThat(new File(new Path(stagingDir).toUri().getPath()).exists()).isFalse();

    Map<String, GenericRecord> rows = readAll(dir, names, golden("arangodb-document.avsc"));
    assertThat(rows.keySet()).containsExactly("k1", "k2", "k3").inOrder();
    for (GenericRecord r : rows.values()) {
      assertEnvelope(r, "user_profiles");
      assertThat(((GenericRecord) r.get("payload")).getSchema().getFields()).hasSize(4);
    }
    GenericRecord k1 = (GenericRecord) rows.get("k1").get("payload");
    assertThat(str(k1.get("id"))).isEqualTo("user-profiles/k1");
    assertThat(str(k1.get("rev"))).isEqualTo("_r1");
    assertThat(str(k1.get("doc"))).isEqualTo("{\"_key\":\"k1\"}");
    GenericRecord k2 = (GenericRecord) rows.get("k2").get("payload");
    assertThat(k2.get("id")).isNull();
    assertThat(k2.get("rev")).isNull();
    assertThat(str(k2.get("doc"))).isEqualTo("{\"_key\":\"k2\",\"a\":[1,null]}");
    assertThat(str(((GenericRecord) rows.get("k3").get("payload")).get("doc")))
        .isEqualTo(unicodeDoc);

    // The summary readback counts the final files.
    ExportReadback.Result r = ExportReadback.read(spark, tableDir, root);
    assertThat(r.rowCount).isEqualTo(3);
    assertThat(r.files.get(0).path).isEqualTo("user_profiles/" + names.get(0));
    assertThat(r.files.get(0).rows).isEqualTo(2);
    assertThat(r.files.get(1).rows).isEqualTo(1);

    // The staging root is empty now and is removed before the summary is written.
    CollectionExporter.deleteStagingRoot(root, spark.sparkContext().hadoopConfiguration());
    assertThat(new File(new Path(root).toUri().getPath(), "_staging").exists()).isFalse();
  }

  @Test
  public void edgeCollection() throws Exception {
    String root = "file://" + tmp.newFolder("edges").getAbsolutePath();
    CollectionPlan plan =
        CollectionPlan.create(new CollectionInfo("knows", CollectionKind.EDGE), "knows", 1);
    String stagingDir = CollectionExporter.stagingDir(root, plan);
    String tableDir = CollectionExporter.tableDir(root, plan);
    ExportReadbackTest.writeBatch(
        spark,
        plan,
        Arrays.asList(
            RowFactory.create(
                "e1",
                "knows/e1",
                "_e1",
                "user-profiles/u1",
                "user-profiles/u2",
                "{\"_key\":\"e1\",\"_from\":\"user-profiles/u1\",\"_to\":\"user-profiles/u2\"}"),
            RowFactory.create(
                "e2",
                null,
                null,
                "user-profiles/u2",
                "missing/dangling",
                "{\"_key\":\"e2\",\"w\":0.5}")),
        CollectionExporter.batchDir(stagingDir, 0));

    CollectionExporter.commit(
        stagingDir, tableDir, plan, spark.sparkContext().hadoopConfiguration());

    File dir = new File(new Path(tableDir).toUri().getPath());
    String md5 = "a7f80905c6bc6e9912edef8c18d80a52";
    List<String> names = sortedNames(dir);
    assertThat(names).containsExactly(md5 + "-00000-of-00001.avro");

    Map<String, GenericRecord> rows = readAll(dir, names, golden("arangodb-edge.avsc"));
    assertThat(rows.keySet()).containsExactly("e1", "e2").inOrder();
    for (GenericRecord r : rows.values()) {
      assertEnvelope(r, "knows");
    }
    GenericRecord e1 = (GenericRecord) rows.get("e1").get("payload");
    assertThat(e1.getSchema().getFields()).hasSize(6);
    assertThat(str(e1.get("id"))).isEqualTo("knows/e1");
    assertThat(str(e1.get("rev"))).isEqualTo("_e1");
    assertThat(str(e1.get("from_id"))).isEqualTo("user-profiles/u1");
    assertThat(str(e1.get("to_id"))).isEqualTo("user-profiles/u2");
    GenericRecord e2 = (GenericRecord) rows.get("e2").get("payload");
    assertThat(e2.get("id")).isNull();
    assertThat(str(e2.get("to_id"))).isEqualTo("missing/dangling");
    assertThat(str(e2.get("doc"))).isEqualTo("{\"_key\":\"e2\",\"w\":0.5}");
  }
}
