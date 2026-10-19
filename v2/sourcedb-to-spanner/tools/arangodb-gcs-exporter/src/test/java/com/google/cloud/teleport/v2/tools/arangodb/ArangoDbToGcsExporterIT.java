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
import static org.junit.Assert.assertThrows;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.util.RawJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * End-to-end test against a real ArangoDB 3.11.14 server (Testcontainers; needs Docker). Run with
 * {@code mvn verify -Pit}.
 */
@RunWith(JUnit4.class)
public class ArangoDbToGcsExporterIT {

  private static final String IMAGE = "arangodb/arangodb:3.11.14";
  private static final String PASSWORD = "test";
  private static final int USER_DOCS = 3_000;
  private static final String KEY_ALPHABET =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-:.@()+,=;$!*'%";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static GenericContainer<?> arango;
  private static ArangoDB client;
  private static ArangoDatabase db;
  private static SparkSession spark;
  private static int userProfilesCount;
  private static int hostileCount;

  @BeforeClass
  public static void setUp() {
    arango =
        new GenericContainer<>(IMAGE)
            .withEnv("ARANGO_ROOT_PASSWORD", PASSWORD)
            .withExposedPorts(8529)
            .waitingFor(
                Wait.forHttp("/_api/version")
                    .withBasicCredentials("root", PASSWORD)
                    .forStatusCode(200));
    arango.start();
    client =
        new ArangoDB.Builder()
            .host(arango.getHost(), arango.getMappedPort(8529))
            .user("root")
            .password(PASSWORD)
            .protocol(Protocol.HTTP_JSON)
            .build();
    db = client.db("_system");
    seed();
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("ArangoDbToGcsExporterIT")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
    if (client != null) {
      client.shutdown();
    }
    if (arango != null) {
      arango.stop();
    }
  }

  private static void seed() {
    db.createCollection("user-profiles");
    List<String> docs = new ArrayList<>();
    docs.add("{\"_key\":\"nested\",\"a\":{\"b\":{\"c\":[1,{\"d\":null}]}}}");
    docs.add("{\"_key\":\"mixed\",\"arr\":[1,\"two\",3.5,true,null,{\"x\":[]},[]]}");
    docs.add("{\"_key\":\"empty\",\"o\":{},\"a\":[]}");
    docs.add("{\"_key\":\"nulls\",\"present\":null}");
    docs.add("{\"_key\":\"missing\"}");
    docs.add(
        "{\"_key\":\"numbers\",\"i\":1,\"f\":1.5,\"s\":\"1\",\"big\":9007199254740993,"
            + "\"max\":9223372036854775807,\"tenth\":0.1,\"huge\":1e300,\"negzero\":-0.0}");
    docs.add("{\"_key\":\"emoji\",\"e\":\"\\ud83d\\ude00 caf\\u00e9\"}");
    docs.add("{\"_key\":\"names\",\"a-b\":1,\"a.b\":2,\"a b\":3}");
    StringBuilder longKey = new StringBuilder();
    while (longKey.length() < 254) {
      longKey.append('k');
    }
    docs.add("{\"_key\":\"" + longKey + "\",\"long\":true}");
    StringBuilder big = new StringBuilder();
    while (big.length() < 2_000_000) {
      big.append("0123456789abcdef");
    }
    docs.add("{\"_key\":\"big\",\"payload\":\"" + big + "\"}");
    for (int i = 0; i < USER_DOCS; i++) {
      docs.add("{\"_key\":\"u" + i + "\",\"n\":" + i + ",\"name\":\"user " + i + "\"}");
    }
    insertAll("user-profiles", docs);
    userProfilesCount = docs.size();

    db.createCollection("knows", new CollectionCreateOptions().type(CollectionType.EDGES));
    List<String> edges = new ArrayList<>();
    edges.add("{\"_key\":\"e1\",\"_from\":\"user-profiles/u1\",\"_to\":\"user-profiles/u2\"}");
    edges.add("{\"_key\":\"e2\",\"_from\":\"user-profiles/u2\",\"_to\":\"user-profiles/u3\"}");
    edges.add("{\"_key\":\"e3\",\"_from\":\"user-profiles/u3\",\"_to\":\"missing/dangling\"}");
    insertAll("knows", edges);

    db.createCollection("hostile_keys");
    Set<String> keys = new LinkedHashSet<>();
    for (char c : KEY_ALPHABET.toCharArray()) {
      keys.add(String.valueOf(c));
      keys.add("k" + c);
      keys.add(c + "k");
    }
    Random r = new Random(42);
    while (keys.size() < 5_000) {
      int len = 1 + r.nextInt(12);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < len; i++) {
        sb.append(KEY_ALPHABET.charAt(r.nextInt(KEY_ALPHABET.length())));
      }
      keys.add(sb.toString());
    }
    List<String> hostile = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (String k : keys) {
      try {
        hostile.add("{\"_key\":" + mapper.writeValueAsString(k) + "}");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    insertAll("hostile_keys", hostile);
    hostileCount = hostile.size();
  }

  private static void insertAll(String collection, List<String> docs) {
    for (int i = 0; i < docs.size(); i += 500) {
      List<String> chunk = docs.subList(i, Math.min(docs.size(), i + 500));
      db.collection(collection).insertDocuments(RawJson.of("[" + String.join(",", chunk) + "]"));
    }
  }

  private String[] args(String exportId, String... extra) throws Exception {
    File out = tmp.getRoot();
    List<String> a = new ArrayList<>();
    a.add("--endpoints=" + arango.getHost() + ":" + arango.getMappedPort(8529));
    a.add("--password=" + PASSWORD);
    a.add("--protocol=http");
    a.add("--output=file://" + out.getAbsolutePath() + "/exports");
    a.add("--exportId=" + exportId);
    for (String e : extra) {
      a.add(e);
    }
    return a.toArray(new String[0]);
  }

  /**
   * Canonical form for JSON comparison: objects as sorted maps, numbers as normalised decimals (so
   * {@code -0}, {@code -0.0} and {@code 0} or {@code 1.50} and {@code 1.5} compare equal).
   */
  private static Object canonical(JsonNode n) {
    if (n.isObject()) {
      Map<String, Object> m = new TreeMap<>();
      n.fields().forEachRemaining(e -> m.put(e.getKey(), canonical(e.getValue())));
      return m;
    }
    if (n.isArray()) {
      List<Object> l = new ArrayList<>();
      n.forEach(c -> l.add(canonical(c)));
      return l;
    }
    if (n.isNumber()) {
      BigDecimal d = n.decimalValue();
      return d.signum() == 0 ? "0" : d.stripTrailingZeros().toString();
    }
    if (n.isNull()) {
      return "<null>";
    }
    return n.isTextual() ? "s:" + n.textValue() : n.toString();
  }

  /** Pattern of a bulk GCS data file name of {@code table}, relative to the export root. */
  private static String bulkFilePattern(ExportSummary.Table t) {
    String md5 =
        "EDGE".equals(t.collectionType)
            ? CollectionPlan.md5Hex(CollectionPlan.edgeAvroSchema())
            : CollectionPlan.md5Hex(CollectionPlan.documentAvroSchema());
    return t.name + "/" + md5 + "-\\d{5}-of-\\d{5}\\.avro";
  }

  /**
   * Reads a table's bulk files (every file in {@code <root>/<table>/}, which must be exactly the
   * summary's files) and returns the payload rows by key, after checking the envelope fields.
   */
  private static Map<String, Row> readTable(String exportRoot, ExportSummary.Table t) {
    List<String> paths = new ArrayList<>();
    Set<String> listed = new TreeSet<>();
    for (ExportSummary.FileEntry f : t.files) {
      assertThat(f.path).matches(bulkFilePattern(t));
      paths.add(exportRoot + "/" + f.path);
      listed.add(f.path.substring(f.path.indexOf('/') + 1));
    }
    String[] onDisk = new File(URI.create(exportRoot + "/" + t.name)).list();
    assertThat(new TreeSet<>(Arrays.asList(onDisk))).isEqualTo(listed);
    Map<String, Row> byKey = new HashMap<>();
    for (Row row : spark.read().format("avro").load(paths.toArray(new String[0])).collectAsList()) {
      assertThat((String) row.getAs("tableName")).isEqualTo(t.name);
      assertThat((Object) row.getAs("shardId")).isNull();
      assertThat(row.getList(row.fieldIndex("primaryKeys"))).containsExactly("key");
      Row payload = row.getStruct(row.fieldIndex("payload"));
      byKey.put(payload.getAs("key"), payload);
    }
    return byKey;
  }

  @Test
  public void exportsAllCollectionsLosslessly() throws Exception {
    ExporterOptions opts =
        ExporterOptions.parse(
            args(
                "full",
                "--collections=user-profiles,knows",
                "--partitions=3",
                "--parallelWrites=2"));
    ExportSummary m = ArangoDbToGcsExporter.run(opts, spark);

    assertThat(m.source.consistent).isTrue();
    assertThat(m.tables).hasSize(2);
    assertThat(m.tables.get(0).name).isEqualTo("user_profiles");
    assertThat(m.tables.get(1).name).isEqualTo("knows");
    for (ExportSummary.Table t : m.tables) {
      long sum = 0;
      for (ExportSummary.FileEntry f : t.files) {
        sum += f.rows;
      }
      assertThat(sum).isEqualTo(t.rowCount);
    }
    assertThat(m.tables.get(0).rowCount).isEqualTo((long) userProfilesCount);
    assertThat(m.tables.get(1).rowCount).isEqualTo(3L);
    assertThat(m.tables.get(0).stats.maxDocBytes).isGreaterThan(2_000_000L);

    ObjectMapper mapper = new ObjectMapper();
    String root = opts.exportRoot();
    Map<String, Row> users = readTable(root, m.tables.get(0));
    assertThat(users).hasSize(userProfilesCount);
    for (Map.Entry<String, Row> e : users.entrySet()) {
      String raw = db.collection("user-profiles").getDocument(e.getKey(), RawJson.class).get();
      assertWithMessage(e.getKey())
          .that(canonical(mapper.readTree((String) e.getValue().getAs("doc"))))
          .isEqualTo(canonical(mapper.readTree(raw)));
      assertThat((String) e.getValue().getAs("id")).isEqualTo("user-profiles/" + e.getKey());
    }
    Map<String, Row> knows = readTable(root, m.tables.get(1));
    assertThat((String) knows.get("e3").getAs("from_id")).isEqualTo("user-profiles/u3");
    assertThat((String) knows.get("e3").getAs("to_id")).isEqualTo("missing/dangling");

    assertThat(new File(tmp.getRoot(), "exports/full/arangodb-export.json").isFile()).isTrue();
    assertThat(new File(tmp.getRoot(), "exports/full/_staging").exists()).isFalse();
    assertThat(new File(tmp.getRoot(), "exports/full/spanner-ddl-googlesql.sql").isFile()).isTrue();
    assertThat(new File(tmp.getRoot(), "exports/full/spanner-ddl-postgresql.sql").isFile())
        .isTrue();

    // Same exportId again is refused.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArangoDbToGcsExporter.run(
                ExporterOptions.parse(args("full", "--collections=knows")), spark));
  }

  @Test
  public void hostileKeyRangesAreDisjointAndComplete() throws Exception {
    // Plan + verify only (A1/A5): sum of per-range AQL counts == collection count.
    assertThat(
            ArangoDbToGcsExporter.run(
                ExporterOptions.parse(
                    args(
                        "plan",
                        "--collections=hostile_keys",
                        "--partitions=40",
                        "--verifyPartitions=true",
                        "--planOnly=true")),
                spark))
        .isNull();
    // And the real export of the same ranges.
    ExportSummary m =
        ArangoDbToGcsExporter.run(
            ExporterOptions.parse(
                args(
                    "hostile",
                    "--collections=hostile_keys",
                    "--partitions=40",
                    "--rangesPerWrite=7",
                    "--verifyPartitions=true")),
            spark);
    assertThat(m.tables.get(0).rowCount).isEqualTo((long) hostileCount);
    assertThat(readTable(ExporterOptions.parse(args("hostile")).exportRoot(), m.tables.get(0)))
        .hasSize(hostileCount);
  }

  @Test
  public void hashPartitioningExportsEverything() throws Exception {
    ExportSummary m =
        ArangoDbToGcsExporter.run(
            ExporterOptions.parse(
                args(
                    "hash",
                    "--collections=hostile_keys",
                    "--partitioning=HASH",
                    "--partitions=4",
                    "--verifyPartitions=true")),
            spark);
    assertThat(m.tables.get(0).rowCount).isEqualTo((long) hostileCount);
  }

  @Test
  public void writesDuringExportFailWithoutSummary() throws Exception {
    db.createCollection("live");
    List<String> docs = new ArrayList<>();
    for (int i = 0; i < 20_000; i++) {
      docs.add("{\"_key\":\"l" + i + "\",\"pad\":\"" + "x".repeat(200) + "\"}");
    }
    insertAll("live", docs);
    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicInteger inserted = new AtomicInteger();
    Thread writer =
        new Thread(
            () -> {
              while (!stop.get()) {
                db.collection("live")
                    .insertDocument(
                        RawJson.of("{\"_key\":\"w" + inserted.incrementAndGet() + "\"}"));
                try {
                  Thread.sleep(5);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    writer.start();
    try {
      assertThrows(
          ExportException.class,
          () ->
              ArangoDbToGcsExporter.run(
                  ExporterOptions.parse(args("live", "--collections=live", "--partitions=3")),
                  spark));
    } finally {
      stop.set(true);
      writer.join();
    }
    assertThat(inserted.get()).isGreaterThan(0);
    assertThat(new File(tmp.getRoot(), "exports/live/arangodb-export.json").exists()).isFalse();
  }
}
