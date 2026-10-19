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
package com.google.cloud.teleport.v2.source.gcs.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsIoWrapperHelper}. */
@RunWith(JUnit4.class)
public class GcsIoWrapperHelperTest {

  private static final String ROOT = "gs://b/exp";
  private static final List<String> EXPORT_TABLES = List.of("knows", "user_profiles");

  @Test
  public void testEmptySelectionSelectsAll() {
    assertThat(GcsIoWrapperHelper.selectTables(ROOT, EXPORT_TABLES, List.of()).keySet())
        .containsExactly("knows", "user_profiles")
        .inOrder();
    assertThat(GcsIoWrapperHelper.selectTables(ROOT, EXPORT_TABLES, null)).hasSize(2);
  }

  @Test
  public void testCaseInsensitiveMatchKeyedByRequestedName() {
    ImmutableMap<String, String> selected =
        GcsIoWrapperHelper.selectTables(ROOT, EXPORT_TABLES, List.of("KNOWS", "missing"));
    assertThat(selected).containsExactly("KNOWS", "knows");
  }

  @Test
  public void testUnknownOnlySelectsNothing() {
    assertThat(GcsIoWrapperHelper.selectTables(ROOT, EXPORT_TABLES, List.of("nope"))).isEmpty();
  }

  @Test
  public void testAllExportTablesMigratedPasses() {
    assertThat(
            GcsIoWrapperHelper.checkAllExportTablesMigrated(
                ROOT, EXPORT_TABLES, List.of("KNOWS", "user_profiles", "other"), false))
        .isEmpty();
  }

  @Test
  public void testUnmigratedExportTableFailsUnlessTablesConfigured() {
    InvalidOptionsException e =
        assertThrows(
            InvalidOptionsException.class,
            () ->
                GcsIoWrapperHelper.checkAllExportTablesMigrated(
                    ROOT, EXPORT_TABLES, List.of("user_profiles"), false));
    assertThat(e).hasMessageThat().contains("[knows]");
    assertThat(
            GcsIoWrapperHelper.checkAllExportTablesMigrated(
                ROOT, EXPORT_TABLES, List.of("user_profiles"), true))
        .containsExactly("knows");
  }

  @Test
  public void testBuildTableSchemaKeepsPayloadSchemaFieldForField() {
    Schema json = SchemaBuilder.builder().stringType();
    json.addProp("logicalType", "json");
    Schema ts = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema payload =
        SchemaBuilder.record("payload")
            .fields()
            .name("key")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .name("n")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .name("at")
            .type(Schema.createUnion(Schema.create(Schema.Type.NULL), ts))
            .noDefault()
            .name("doc")
            .type(Schema.createUnion(Schema.create(Schema.Type.NULL), json))
            .noDefault()
            .endRecord();
    SourceTableSchema schema =
        GcsIoWrapperHelper.buildTableSchema(
            "Knows", GcsExportTable.create("knows", payload, ImmutableList.of("key")));
    assertThat(schema.tableName()).isEqualTo("Knows");
    assertThat(schema.primaryKeyColumns()).containsExactly("key");
    assertThat(schema.getAvroPayload()).isEqualTo(payload);
    assertThat(schema.getAvroPayload().toString()).isEqualTo(payload.toString());
    assertThat(schema.sourceColumnNameToSourceColumnType().get("key").getName())
        .isEqualTo("STRING");
    assertThat(schema.sourceColumnNameToSourceColumnType().get("n").getName()).isEqualTo("LONG");
    assertThat(schema.sourceColumnNameToSourceColumnType().get("at").getName())
        .isEqualTo("TIMESTAMP");
    assertThat(schema.sourceColumnNameToSourceColumnType().get("doc").getName()).isEqualTo("JSON");
  }
}
