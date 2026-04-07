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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for SpannerChangeEventContext. */
public final class SpannerChangeEventContextTest {

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void canGenerateShadowTableMutation() throws Exception {
    long timestamp = 1712502692437633L;
    long recordSequence = 1L;
    long modNumber = 0L;

    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("userid")
            .int64()
            .endColumn()
            .column("firstname")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("userid")
            .end()
            .endTable()
            .build();

    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_Users")
            .column("userid")
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
            .asc("userid")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.SPANNER_SOURCE_TYPE);
    changeEvent.put("userid", 1);
    changeEvent.put("firstname", "Alice");

    changeEvent.put("_metadata_commit_timestamp", timestamp);
    changeEvent.put("_metadata_record_sequence", recordSequence);
    changeEvent.put("_metadata_mod_number", modNumber);

    String jsonStr = changeEvent.toString();
    JsonNode node = getJsonNode(jsonStr);
    org.junit.Assert.assertNotNull(
        "_metadata_commit_timestamp should not be null", node.get("_metadata_commit_timestamp"));

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            node, ddl, shadowDdl, "shadow_", DatastreamConstants.SPANNER_SOURCE_TYPE);

    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    org.junit.Assert.assertEquals(Value.int64(1), actual.get("userid"));
    org.junit.Assert.assertEquals(Value.int64(timestamp), actual.get("timestamp"));
    org.junit.Assert.assertEquals(Value.int64(recordSequence), actual.get("record_sequence"));
    org.junit.Assert.assertEquals(Value.int64(modNumber), actual.get("mod_number"));

    assertThat(changeEventContext, instanceOf(SpannerChangeEventContext.class));
    assertEquals("shadow_Users", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
  }
}
