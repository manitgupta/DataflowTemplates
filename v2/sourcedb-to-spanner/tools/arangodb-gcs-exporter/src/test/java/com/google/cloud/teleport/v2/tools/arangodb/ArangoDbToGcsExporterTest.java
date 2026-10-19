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
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArangoDbToGcsExporterTest {

  private static final List<CollectionInfo> AVAILABLE =
      Arrays.asList(
          new CollectionInfo("_graphs", CollectionKind.DOCUMENT),
          new CollectionInfo("knows", CollectionKind.EDGE),
          new CollectionInfo("user-profiles", CollectionKind.DOCUMENT),
          new CollectionInfo("orders", CollectionKind.DOCUMENT));

  @Test
  public void selectsAllNonSystemByDefault() {
    List<CollectionInfo> s =
        ArangoDbToGcsExporter.selectCollections(
            AVAILABLE, Collections.emptyList(), Collections.emptyList());
    assertThat(s).containsExactly(AVAILABLE.get(1), AVAILABLE.get(2), AVAILABLE.get(3)).inOrder();
  }

  @Test
  public void includeAndExclude() {
    List<CollectionInfo> s =
        ArangoDbToGcsExporter.selectCollections(
            AVAILABLE, Arrays.asList("orders", "knows", "orders"), Arrays.asList("knows"));
    assertThat(s).containsExactly(AVAILABLE.get(3));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArangoDbToGcsExporter.selectCollections(
                AVAILABLE, Arrays.asList("missing"), Collections.emptyList()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArangoDbToGcsExporter.selectCollections(
                AVAILABLE, Arrays.asList("_graphs"), Collections.emptyList()));
  }

  private static ExportSummary.Table table(
      long rows, long before, long after, String revBefore, String revAfter) {
    ExportSummary.Table t = new ExportSummary.Table();
    t.name = "t";
    t.sourceName = "t";
    t.rowCount = rows;
    t.sourceCountBefore = before;
    t.sourceCountAfter = after;
    t.sourceRevisionBefore = revBefore;
    t.sourceRevisionAfter = revAfter;
    return t;
  }

  @Test
  public void consistency() {
    assertThat(
            ArangoDbToGcsExporter.consistencyProblems(
                Collections.singletonList(table(5, 5, 5, "r1", "r1"))))
        .isEmpty();
    assertThat(
            ArangoDbToGcsExporter.consistencyProblems(
                Collections.singletonList(table(5, 5, 6, "r1", "r2"))))
        .hasSize(2);
    assertThat(
            ArangoDbToGcsExporter.consistencyProblems(
                Collections.singletonList(table(5, 5, 5, "r1", "r2"))))
        .hasSize(1);
    List<String> p =
        ArangoDbToGcsExporter.consistencyProblems(
            Collections.singletonList(table(4, 5, 5, "r1", "r1")));
    assertThat(p).hasSize(1);
    assertThat(p.get(0)).contains("exported 4 rows");
  }

  @Test
  public void producerString() {
    String p = ArangoDbToGcsExporter.producer("3.5.3");
    assertThat(p).startsWith("arangodb-gcs-exporter/");
    assertThat(p).contains("(spark 3.5.3, connector ");
  }

  private static final CollectionInfo HOSTILE =
      new CollectionInfo("hostile_keys", CollectionKind.DOCUMENT);

  /** countQuery text -> rows the simulated server returns for that range. */
  private static Map<String, Long> countsByQuery(
      FakeArangoKeySource server, List<KeyRange> ranges) {
    Map<String, Long> m = new HashMap<>();
    for (KeyRange r : ranges) {
      m.put(AqlBuilder.countQuery(HOSTILE, AqlBuilder.rangeFilter(r)), server.countRange(r));
    }
    return m;
  }

  private static List<String> filters(List<KeyRange> ranges) {
    List<String> f = new ArrayList<>();
    for (KeyRange r : ranges) {
      f.add(AqlBuilder.rangeFilter(r));
    }
    return f;
  }

  @Test
  public void verifyPartitions_passesForPlannedRangesOnHostileKeys() throws Exception {
    List<String> keys = KeyRangePlannerTest.hostileKeys(10_000, 11);
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 11);
    List<KeyRange> ranges = KeyRangePlanner.plan(server, "hostile_keys", keys.size(), 40);
    assertThat(ranges.size()).isGreaterThan(1);
    Map<String, Long> counts = countsByQuery(server, ranges);
    ArangoDbToGcsExporter.verifyPartitions(
        q -> counts.get(q),
        CollectionPlan.create(HOSTILE, "hostile_keys", ranges.size()),
        filters(ranges),
        keys.size(),
        false);
  }

  @Test
  public void verifyPartitions_failsOnOverlappingOrMissingRange() {
    List<String> keys = KeyRangePlannerTest.hostileKeys(10_000, 12);
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 12);
    List<KeyRange> planned = KeyRangePlanner.plan(server, "hostile_keys", keys.size(), 40);
    CollectionPlan plan = CollectionPlan.create(HOSTILE, "hostile_keys", planned.size());

    // Overlap: the second range starts at the first range's start (unbounded below).
    List<KeyRange> overlapping = new ArrayList<>(planned);
    overlapping.set(1, new KeyRange(null, planned.get(1).hi()));
    Map<String, Long> overlapCounts = countsByQuery(server, overlapping);
    ExportException e =
        assertThrows(
            ExportException.class,
            () ->
                ArangoDbToGcsExporter.verifyPartitions(
                    q -> overlapCounts.get(q), plan, filters(overlapping), keys.size(), false));
    assertThat(e).hasMessageThat().contains("Partition verification failed");

    // Gap: the middle range is missing.
    List<KeyRange> missing = new ArrayList<>(planned);
    missing.remove(planned.size() / 2);
    Map<String, Long> missingCounts = countsByQuery(server, missing);
    assertThrows(
        ExportException.class,
        () ->
            ArangoDbToGcsExporter.verifyPartitions(
                q -> missingCounts.get(q), plan, filters(missing), keys.size(), false));
  }

  @Test
  public void verifyPartitions_allowLiveSourceOnlyLogs() throws Exception {
    ArangoDbToGcsExporter.verifyPartitions(
        q -> 3L,
        CollectionPlan.create(HOSTILE, "hostile_keys", 2),
        Arrays.asList("FILTER d._key < \"m\"", "FILTER d._key >= \"m\""),
        5,
        true);
  }

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static String uri(File f) {
    return f.toURI().toString().replaceAll("/$", "");
  }

  @Test
  public void checkNoExistingExport_refusesSummaryOrData() throws Exception {
    Configuration conf = new Configuration();
    File fresh = tmp.newFolder("fresh");
    ArangoDbToGcsExporter.checkNoExistingExport(uri(fresh), conf);
    ArangoDbToGcsExporter.checkNoExistingExport(
        uri(new File(tmp.getRoot(), "does-not-exist")), conf);

    File withTables = tmp.newFolder("withTables");
    new File(withTables, "_staging/t/b00000").mkdirs();
    IllegalArgumentException e1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> ArangoDbToGcsExporter.checkNoExistingExport(uri(withTables), conf));
    assertThat(e1).hasMessageThat().contains("earlier incomplete run");

    File withTableDir = tmp.newFolder("withTableDir");
    new File(withTableDir, "users").mkdirs();
    assertThrows(
        IllegalArgumentException.class,
        () -> ArangoDbToGcsExporter.checkNoExistingExport(uri(withTableDir), conf));

    File withSummary = tmp.newFolder("withSummary");
    Files.write(
        new File(withSummary, ExportSummaryWriter.SUMMARY_FILE).toPath(),
        "{}".getBytes(StandardCharsets.UTF_8));
    IllegalArgumentException e2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> ArangoDbToGcsExporter.checkNoExistingExport(uri(withSummary), conf));
    assertThat(e2).hasMessageThat().contains("already has an arangodb-export.json");
  }
}
