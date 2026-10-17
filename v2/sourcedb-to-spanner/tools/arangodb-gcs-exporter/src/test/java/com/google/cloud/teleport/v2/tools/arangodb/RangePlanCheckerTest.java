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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RangePlanCheckerTest {

  private static Map<String, Object> node(String type, String... indexTypes) {
    Map<String, Object> m = new HashMap<>();
    m.put("type", type);
    if (indexTypes.length > 0) {
      List<Map<String, Object>> indexes = new ArrayList<>();
      for (String t : indexTypes) {
        indexes.add(Collections.singletonMap("type", t));
      }
      m.put("indexes", indexes);
    }
    return m;
  }

  /** Shape of a 3.11 cluster plan for a covered primary-index range query. */
  private static List<Map<String, Object>> goodPlan() {
    return Arrays.asList(
        node("SingletonNode"),
        node("IndexNode", "primary"),
        node("CalculationNode"),
        node("RemoteNode"),
        node("GatherNode"),
        node("ReturnNode"));
  }

  @Test
  public void goodPlanHasNoProblems() {
    assertThat(RangePlanChecker.problems(goodPlan())).isEmpty();
  }

  @Test
  public void residualFilterFullScanOrOtherIndexAreProblems() {
    List<Map<String, Object>> residual = new ArrayList<>(goodPlan());
    residual.add(2, node("FilterNode"));
    assertThat(RangePlanChecker.problems(residual)).hasSize(1);
    assertThat(RangePlanChecker.problems(residual).get(0)).contains("FilterNode");

    List<Map<String, Object>> fullScan =
        Arrays.asList(
            node("SingletonNode"),
            node("EnumerateCollectionNode"),
            node("CalculationNode"),
            node("FilterNode"),
            node("ReturnNode"));
    assertThat(RangePlanChecker.problems(fullScan)).hasSize(3);

    List<Map<String, Object>> postFiltered = new ArrayList<>(goodPlan());
    Map<String, Object> indexNode = new HashMap<>(postFiltered.get(1));
    indexNode.put("filter", Collections.singletonMap("type", "n-ary and"));
    postFiltered.set(1, indexNode);
    assertThat(RangePlanChecker.problems(postFiltered)).hasSize(1);
    assertThat(RangePlanChecker.problems(postFiltered).get(0)).contains("post-filter");

    List<Map<String, Object>> otherIndex =
        Arrays.asList(node("SingletonNode"), node("IndexNode", "persistent"), node("ReturnNode"));
    assertThat(RangePlanChecker.problems(otherIndex))
        .containsExactly("no IndexNode on the primary index");
  }

  @Test
  public void samplesFirstMiddleLast() {
    assertThat(RangePlanChecker.sampleQueries(Collections.singletonList("q"))).isEmpty();
    assertThat(RangePlanChecker.sampleQueries(Arrays.asList("a", "b"))).containsExactly("a", "b");
    assertThat(RangePlanChecker.sampleQueries(Arrays.asList("a", "b", "c", "d", "e")))
        .containsExactly("a", "c", "e")
        .inOrder();
  }

  @Test
  public void checkFailsWithEveryProblemAndPassesOtherwise() {
    List<String> queries = Arrays.asList("q0", "q1", "q2");
    RangePlanChecker.check("c", queries, q -> goodPlan());
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                RangePlanChecker.check(
                    "c",
                    queries,
                    q ->
                        q.equals("q2")
                            ? Arrays.asList(node("EnumerateCollectionNode"), node("FilterNode"))
                            : goodPlan()));
    assertThat(e).hasMessageThat().contains("in: q2");
    assertThat(e).hasMessageThat().doesNotContain("in: q0");
    assertThat(e).hasMessageThat().contains("--verifyRangePlans=false");
  }
}
