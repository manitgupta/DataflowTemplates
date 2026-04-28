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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;

/**
 * Spanner implementation of ChangeEventContext that provides implementation of the
 * generateShadowTableMutation method.
 */
class SpannerChangeEventContext extends ChangeEventContext {

  public SpannerChangeEventContext(
      JsonNode changeEvent, Ddl ddl, Ddl shadowTableDdl, String shadowTablePrefix)
      throws ChangeEventConvertorException, InvalidChangeEventException, DroppedTableException {
    super(changeEvent, ddl, DatastreamConstants.SPANNER_SORT_ORDER);
    this.changeEvent = changeEvent;
    this.shadowTablePrefix = shadowTablePrefix;
    this.dataTable = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY).asText();
    this.shadowTable = shadowTablePrefix + this.dataTable;

    convertChangeEventToMutation(ddl, shadowTableDdl);
  }

  /*
   * Creates shadow table mutation for Spanner.
   */
  @Override
  Mutation generateShadowTableMutation(Ddl ddl, Ddl shadowDdl)
      throws ChangeEventConvertorException {
    // Get shadow information from change event mutation context
    Mutation.WriteBuilder builder =
        ChangeEventConvertor.changeEventToShadowTableMutationBuilder(
            shadowDdl, changeEvent, shadowTablePrefix);

    // Add sort information to shadow table mutation
    JsonNode commitTimestamp = changeEvent.get("_metadata_commit_timestamp");
    JsonNode recordSequence = changeEvent.get("_metadata_record_sequence");
    JsonNode modNumber = changeEvent.get("_metadata_mod_number");

    if (commitTimestamp == null || recordSequence == null || modNumber == null) {
      throw new ChangeEventConvertorException("Missing Spanner metadata in change event");
    }

    builder
        .set(getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_TIMESTAMP_KEY))
        .to(Value.int64(commitTimestamp.asLong()));
    builder
        .set(getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_RECORD_SEQUENCE_KEY))
        .to(Value.int64(recordSequence.asLong()));
    builder
        .set(getSafeShadowColumn(DatastreamConstants.SPANNER_SORT_ORDER_MOD_NUMBER_KEY))
        .to(Value.int64(modNumber.asLong()));

    return builder.build();
  }
}
