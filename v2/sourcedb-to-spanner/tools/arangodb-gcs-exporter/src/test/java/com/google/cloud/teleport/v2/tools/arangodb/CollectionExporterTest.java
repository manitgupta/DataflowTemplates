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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CollectionExporterTest {

  private static final CollectionInfo USERS = new CollectionInfo("users", CollectionKind.DOCUMENT);

  @Test
  public void batches() {
    List<Integer> items = new ArrayList<>();
    for (int i = 0; i < 130; i++) {
      items.add(i);
    }
    List<List<Integer>> b = CollectionExporter.batches(items, 64);
    assertThat(b).hasSize(3);
    assertThat(b.get(0)).hasSize(64);
    assertThat(b.get(1)).hasSize(64);
    assertThat(b.get(2)).containsExactly(128, 129).inOrder();
    assertThat(CollectionExporter.batches(Collections.emptyList(), 3)).isEmpty();
    assertThat(CollectionExporter.batches(Arrays.asList(1), 3)).hasSize(1);
    assertThrows(
        IllegalArgumentException.class, () -> CollectionExporter.batches(Arrays.asList(1), 0));
  }

  @Test
  public void endpointForRotatesOverAllCoordinators() {
    java.util.Map<String, String> opts =
        java.util.Collections.singletonMap("endpoints", "c0:8529, c1:8529,c2:8529");
    assertThat(CollectionExporter.endpointFor(opts, 0)).isEqualTo("c0:8529");
    assertThat(CollectionExporter.endpointFor(opts, 1)).isEqualTo("c1:8529");
    assertThat(CollectionExporter.endpointFor(opts, 2)).isEqualTo("c2:8529");
    assertThat(CollectionExporter.endpointFor(opts, 3)).isEqualTo("c0:8529");
    assertThat(CollectionExporter.endpointFor(opts, 64 * 7 + 5)).isEqualTo("c0:8529");
    assertThat(
            CollectionExporter.endpointFor(
                java.util.Collections.singletonMap("endpoints", "only:8529"), 41))
        .isEqualTo("only:8529");
  }

  @Test
  public void directories() {
    CollectionPlan plan = CollectionPlan.create(USERS, "users", 1);
    String dir = CollectionExporter.tableDir("gs://b/e/x1", plan);
    assertThat(dir).isEqualTo("gs://b/e/x1/tables/users");
    assertThat(CollectionExporter.batchDir(dir, 7)).isEqualTo("gs://b/e/x1/tables/users/b00007");
  }

  @Test
  public void singlePartitionIsOneUnfilteredQuery() {
    FakeArangoKeySource server = new FakeArangoKeySource(Arrays.asList("a", "b"), 1);
    List<String> q =
        CollectionExporter.planQueries(
            CollectionPlan.create(USERS, "users", 1), PartitioningMode.RANGE, server, 2);
    assertThat(q).containsExactly(AqlBuilder.rangeQuery(USERS, KeyRange.ALL));
    assertThat(server.sampleCalls).isEqualTo(0);
  }

  @Test
  public void hashQueries() {
    FakeArangoKeySource server = new FakeArangoKeySource(Arrays.asList("a", "b"), 1);
    List<String> q =
        CollectionExporter.planQueries(
            CollectionPlan.create(USERS, "users", 4), PartitioningMode.HASH, server, 2);
    assertThat(q).hasSize(4);
    assertThat(q.get(3)).contains("FILTER HASH(d._key) % 4 == 3");
    assertThat(server.sampleCalls).isEqualTo(0);
  }

  @Test
  public void rangeQueries() {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      keys.add(String.format("u%05d", i));
    }
    FakeArangoKeySource server = new FakeArangoKeySource(keys, 1);
    List<String> q =
        CollectionExporter.planQueries(
            CollectionPlan.create(USERS, "users", 5), PartitioningMode.RANGE, server, keys.size());
    assertThat(q).hasSize(5);
    assertThat(q.get(0)).contains("FILTER d._key < \"u");
    assertThat(q.get(2)).contains("FILTER d._key >= \"u");
    assertThat(q.get(2)).contains(" AND d._key < \"u");
    assertThat(q.get(4)).doesNotContain(" AND ");
    assertThat(server.sampleCalls).isEqualTo(1);
  }

  @Test
  public void runAllFailFast_returnsResultsInSubmissionOrder() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      List<Callable<Integer>> tasks = new ArrayList<>();
      for (int i = 0; i < 6; i++) {
        final int v = i;
        tasks.add(
            () -> {
              Thread.sleep(10L * (6 - v));
              return v;
            });
      }
      AtomicBoolean failed = new AtomicBoolean();
      assertThat(CollectionExporter.runAllFailFast(pool, tasks, () -> failed.set(true)))
          .containsExactly(0, 1, 2, 3, 4, 5)
          .inOrder();
      assertThat(failed.get()).isFalse();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void runAllFailFast_reactsToALaterFailureWithoutWaitingForEarlierTasks() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch release = new CountDownLatch(1);
    try {
      AtomicBoolean failed = new AtomicBoolean();
      AtomicBoolean firstInterrupted = new AtomicBoolean();
      List<Callable<Integer>> tasks =
          Arrays.asList(
              () -> {
                try {
                  release.await(); // would block forever without fail-fast cancellation
                } catch (InterruptedException e) {
                  firstInterrupted.set(true);
                  throw e;
                }
                return 0;
              },
              () -> {
                throw new IllegalStateException("batch 1 failed");
              });
      long start = System.nanoTime();
      IllegalStateException e =
          assertThrows(
              IllegalStateException.class,
              () -> CollectionExporter.runAllFailFast(pool, tasks, () -> failed.set(true)));
      assertThat(e).hasMessageThat().isEqualTo("batch 1 failed");
      assertThat(failed.get()).isTrue();
      assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(10));
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      assertThat(firstInterrupted.get()).isTrue();
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }
}
