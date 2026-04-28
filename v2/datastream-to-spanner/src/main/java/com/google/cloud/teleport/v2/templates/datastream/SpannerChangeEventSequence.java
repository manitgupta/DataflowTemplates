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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerReadUtils;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ChangeEventSequence for Spanner database which stores change event sequence
 * information and implements the comparison method.
 */
class SpannerChangeEventSequence extends ChangeEventSequence {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeEventSequence.class);

  private final Long timestamp;
  private final Long recordSequence;
  private final Long modNumber;

  SpannerChangeEventSequence(Long timestamp, Long recordSequence, Long modNumber) {
    super(DatastreamConstants.SPANNER_SOURCE_TYPE);
    this.timestamp = timestamp;
    this.recordSequence = recordSequence;
    this.modNumber = modNumber;
  }

  public static SpannerChangeEventSequence createFromChangeEvent(ChangeEventContext ctx)
      throws ChangeEventConvertorException, InvalidChangeEventException {
    JsonNode event = ctx.getChangeEvent();
    JsonNode commitTimestamp = event.get("_metadata_commit_timestamp");
    JsonNode recordSequence = event.get("_metadata_record_sequence");
    JsonNode modNumber = event.get("_metadata_mod_number");

    if (commitTimestamp == null || recordSequence == null || modNumber == null) {
      throw new InvalidChangeEventException("Missing Spanner metadata in change event");
    }

    return new SpannerChangeEventSequence(
        commitTimestamp.asLong(), recordSequence.asLong(), modNumber.asLong());
  }

  public static SpannerChangeEventSequence createFromShadowTable(
      final TransactionContext transactionContext,
      ChangeEventContext context,
      Ddl shadowTableDdl,
      boolean useSqlStatements)
      throws ChangeEventSequenceCreationException {

    try {
      String shadowTable = context.getShadowTable();
      Key primaryKey = context.getPrimaryKey();
      List<String> readColumnList =
          java.util.Arrays.asList(
              context.getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_TIMESTAMP_KEY),
              context.getSafeShadowColumn(
                  DatastreamConstants.SPANNER_SORT_ORDER_RECORD_SEQUENCE_KEY),
              context.getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_MOD_NUMBER_KEY));
      Struct row;
      if (useSqlStatements) {
        Statement sql =
            SpannerReadUtils.generateReadSQLWithExclusiveLock(
                shadowTable, readColumnList, primaryKey, shadowTableDdl);
        ResultSet resultSet = transactionContext.executeQuery(sql);
        if (!resultSet.next()) {
          return null;
        }
        row = resultSet.getCurrentRowAsStruct();
      } else {
        row = transactionContext.readRow(shadowTable, primaryKey, readColumnList);
      }
      if (row == null) {
        return null;
      }
      return new SpannerChangeEventSequence(
          row.getLong(readColumnList.get(0)),
          row.getLong(readColumnList.get(1)),
          row.getLong(readColumnList.get(2)));
    } catch (Exception e) {
      throw new ChangeEventSequenceCreationException(e);
    }
  }

  Long getTimestamp() {
    return timestamp;
  }

  Long getRecordSequence() {
    return recordSequence;
  }

  Long getModNumber() {
    return modNumber;
  }

  @Override
  public int compareTo(ChangeEventSequence o) {
    if (!(o instanceof SpannerChangeEventSequence)) {
      throw new ChangeEventSequenceComparisonException(
          "Expected: SpannerChangeEventSequence; Received: " + o.getClass().getSimpleName());
    }
    SpannerChangeEventSequence other = (SpannerChangeEventSequence) o;

    int timestampComparison = this.timestamp.compareTo(other.timestamp);
    if (timestampComparison != 0) {
      return timestampComparison;
    }

    int recordSequenceComparison = this.recordSequence.compareTo(other.recordSequence);
    if (recordSequenceComparison != 0) {
      return recordSequenceComparison;
    }

    return this.modNumber.compareTo(other.modNumber);
  }

  @Override
  public String toString() {
    return "SpannerChangeEventSequence{"
        + "timestamp="
        + timestamp
        + ", recordSequence="
        + recordSequence
        + ", modNumber="
        + modNumber
        + '}';
  }
}
