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

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import com.google.cloud.teleport.v2.source.gcs.reader.GcsAvroTableReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsIoWrapper}. */
@RunWith(JUnit4.class)
public class GcsIoWrapperTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testReadersAndSchema() throws Exception {
    File root = tmp.newFolder("20261017T100000Z");
    GcsTestFixtures.writeExport(root);
    GcsIoWrapper wrapper = new GcsIoWrapper(root.getAbsolutePath() + "/", List.of(), null);

    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        readers = wrapper.getTableReaders();
    assertThat(readers).hasSize(2);

    ImmutableList<SourceSchema> schemas = wrapper.discoverTableSchema();
    assertThat(schemas).hasSize(1);
    SourceSchema schema = schemas.get(0);
    assertThat(schema.schemaReference().getKind()).isEqualTo(SourceSchemaReference.Kind.GCS);
    assertThat(schema.schemaReference().getName()).isEqualTo("GcsExport.20261017T100000Z");
    Map<String, SourceTableSchema> byName =
        schema.tableSchemas().stream()
            .collect(Collectors.toMap(SourceTableSchema::tableName, s -> s));
    assertThat(byName.keySet()).containsExactly("user_profiles", "knows");
    SourceTableSchema knows = byName.get("knows");
    assertThat(knows.sourceColumnNameToSourceColumnType().keySet())
        .containsExactly("key", "id", "rev", "from_id", "to_id", "doc")
        .inOrder();
    assertThat(knows.sourceColumnNameToSourceColumnType().get("doc").getName()).isEqualTo("JSON");
    assertThat(knows.primaryKeyColumns()).containsExactly("key");
    assertThat(knows.getAvroPayload())
        .isEqualTo(GcsTestFixtures.edgeTable("knows").getAvroPayload());

    for (Map.Entry<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        e : readers.entrySet()) {
      assertThat(e.getKey()).hasSize(1);
      SourceTableReference ref = e.getKey().get(0);
      assertThat(ref.sourceSchemaReference()).isEqualTo(schema.schemaReference());
      assertThat(ref.sourceTableSchemaUUID())
          .isEqualTo(byName.get(ref.sourceTableName()).tableSchemaUUID());
      assertThat(e.getValue()).isInstanceOf(GcsAvroTableReader.class);
      SerializableUtils.ensureSerializable(e.getValue());
    }
  }

  @Test
  public void testRequestedTablesOnly() throws Exception {
    String root = GcsTestFixtures.writeExport(tmp.getRoot());
    GcsIoWrapper wrapper = new GcsIoWrapper(root, List.of("USER_PROFILES"), null);
    assertThat(wrapper.getTableReaders()).hasSize(1);
    assertThat(wrapper.getTableReaders().keySet().iterator().next().get(0).sourceTableName())
        .isEqualTo("USER_PROFILES");
  }

  @Test
  public void testUnknownTablesOnlyGivesEmptyReaders() throws Exception {
    String root = GcsTestFixtures.writeExport(tmp.getRoot());
    GcsIoWrapper wrapper = new GcsIoWrapper(root, List.of("nope"), null);
    assertThat(wrapper.getTableReaders()).isEmpty();
    assertThat(wrapper.discoverTableSchema().get(0).tableSchemas()).isEmpty();
  }

  @Test
  public void testEmptyExport() {
    GcsIoWrapper wrapper = new GcsIoWrapper(tmp.getRoot().getAbsolutePath(), List.of(), null);
    assertThat(wrapper.getTableReaders()).isEmpty();
  }

  @Test
  public void testExportId() {
    assertThat(GcsIoWrapper.exportId("gs://b/exports/run1")).isEqualTo("run1");
    assertThat(GcsIoWrapper.exportId("gs://b/exports/run1/")).isEqualTo("run1");
    assertThat(GcsIoWrapper.exportId("run1")).isEqualTo("run1");
  }
}
