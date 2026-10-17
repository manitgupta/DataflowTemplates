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
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsExportManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsTableManifest;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsIoWrapperHelper}. */
@RunWith(JUnit4.class)
public class GcsIoWrapperHelperTest {

  private GcsExportManifest manifest;

  @Before
  public void setUp() throws Exception {
    manifest = GcsExportManifest.parse(GcsTestFixtures.resource("GcsSourceUT/manifest-valid.json"));
  }

  @Test
  public void testEmptySelectionSelectsAll() {
    assertThat(GcsIoWrapperHelper.selectTables(manifest, List.of()).keySet())
        .containsExactly("user_profiles", "knows")
        .inOrder();
    assertThat(GcsIoWrapperHelper.selectTables(manifest, null)).hasSize(2);
  }

  @Test
  public void testCaseInsensitiveMatchKeyedByRequestedName() {
    ImmutableMap<String, GcsTableManifest> selected =
        GcsIoWrapperHelper.selectTables(manifest, List.of("KNOWS", "missing"));
    assertThat(selected.keySet()).containsExactly("KNOWS");
    assertThat(selected.get("KNOWS").getName()).isEqualTo("knows");
  }

  @Test
  public void testUnknownOnlySelectsNothing() {
    assertThat(GcsIoWrapperHelper.selectTables(manifest, List.of("nope"))).isEmpty();
  }

  @Test
  public void testAllManifestTablesMigratedPasses() {
    assertThat(
            GcsIoWrapperHelper.checkAllManifestTablesMigrated(
                manifest, List.of("KNOWS", "user_profiles", "other"), false))
        .isEmpty();
  }

  @Test
  public void testUnmigratedManifestTableFailsUnlessTablesConfigured() {
    InvalidOptionsException e =
        assertThrows(
            InvalidOptionsException.class,
            () ->
                GcsIoWrapperHelper.checkAllManifestTablesMigrated(
                    manifest, List.of("user_profiles"), false));
    assertThat(e).hasMessageThat().contains("[knows]");
    assertThat(
            GcsIoWrapperHelper.checkAllManifestTablesMigrated(
                manifest, List.of("user_profiles"), true))
        .containsExactly("knows");
  }

  @Test
  public void testBuildTableSchema() {
    GcsTableManifest knows = manifest.getTables().get(1);
    SourceTableSchema schema = GcsIoWrapperHelper.buildTableSchema("Knows", knows);
    assertThat(schema.tableName()).isEqualTo("Knows");
    assertThat(schema.primaryKeyColumns()).containsExactly("key");
    assertThat(schema.sourceColumnNameToSourceColumnType().keySet())
        .containsExactly("key", "id", "rev", "from_id", "to_id", "doc")
        .inOrder();
    assertThat(schema.sourceColumnNameToSourceColumnType().get("doc").getName()).isEqualTo("JSON");
    Schema payload = schema.getAvroPayload();
    assertThat(payload.getFields()).hasSize(6);
    assertThat(payload.getField("doc").schema().getTypes().get(1).getProp("logicalType"))
        .isEqualTo("json");
    assertThat(GcsIoWrapperHelper.columnTypes(knows).get("from_id")).isEqualTo("STRING");
    assertThat(GcsIoWrapperHelper.columnTypes(knows).keySet())
        .containsExactly("key", "id", "rev", "from_id", "to_id", "doc")
        .inOrder();
  }
}
