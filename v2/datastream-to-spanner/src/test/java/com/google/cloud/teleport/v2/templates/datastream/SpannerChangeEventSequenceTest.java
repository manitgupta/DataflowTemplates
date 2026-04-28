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
package com.google.cloud.teleport.v2.templates.datastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import org.junit.Test;

/** Unit tests for testing change event comparison logic in Spanner database. */
public final class SpannerChangeEventSequenceTest {

  @Test
  public void canOrderBasedOnTimestamp() {
    SpannerChangeEventSequence oldEvent = new SpannerChangeEventSequence(100L, 1L, 0L);
    SpannerChangeEventSequence newEvent = new SpannerChangeEventSequence(200L, 1L, 0L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnRecordSequence() {
    SpannerChangeEventSequence oldEvent = new SpannerChangeEventSequence(100L, 1L, 0L);
    SpannerChangeEventSequence newEvent = new SpannerChangeEventSequence(100L, 2L, 0L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnModNumber() {
    SpannerChangeEventSequence oldEvent = new SpannerChangeEventSequence(100L, 1L, 0L);
    SpannerChangeEventSequence newEvent = new SpannerChangeEventSequence(100L, 1L, 1L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void testCreateFromShadowTableWithUseSqlStatements() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_table1")
            .column("id")
            .int64()
            .endColumn()
            .column("timestamp")
            .int64()
            .endColumn()
            .column("record_sequence")
            .int64()
            .endColumn()
            .column("mod_number")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    boolean useSqlStatements = true;

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table1");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_TIMESTAMP_KEY))
        .thenReturn("timestamp");
    when(mockContext.getSafeShadowColumn(
            DatastreamConstants.SPANNER_SORT_ORDER_RECORD_SEQUENCE_KEY))
        .thenReturn("record_sequence");
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_MOD_NUMBER_KEY))
        .thenReturn("mod_number");

    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong("timestamp")).thenReturn(100L);
    when(mockRow.getLong("record_sequence")).thenReturn(1L);
    when(mockRow.getLong("mod_number")).thenReturn(0L);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockRow);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    SpannerChangeEventSequence result =
        SpannerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, useSqlStatements);

    assertNotNull(result);
    assertEquals((Object) 100L, result.getTimestamp());
    assertEquals((Object) 1L, result.getRecordSequence());
    assertEquals((Object) 0L, result.getModNumber());
  }

  @Test
  public void testCreateFromChangeEvent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode event = mapper.createObjectNode();
    event.put("_metadata_commit_timestamp", 100L);
    event.put("_metadata_record_sequence", 1L);
    event.put("_metadata_mod_number", 0L);

    ChangeEventContext mockCtx = mock(ChangeEventContext.class);
    when(mockCtx.getChangeEvent()).thenReturn(event);

    SpannerChangeEventSequence result = SpannerChangeEventSequence.createFromChangeEvent(mockCtx);

    assertNotNull(result);
    assertEquals((Object) 100L, result.getTimestamp());
    assertEquals((Object) 1L, result.getRecordSequence());
    assertEquals((Object) 0L, result.getModNumber());
  }
}
