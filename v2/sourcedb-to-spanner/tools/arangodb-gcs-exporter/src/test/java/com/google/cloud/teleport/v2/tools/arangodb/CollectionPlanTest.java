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

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CollectionPlanTest {

  private static List<String> names(StructType t) {
    List<String> names = new ArrayList<>();
    for (StructField f : t.fields()) {
      names.add(f.name());
      assertThat(f.dataType()).isEqualTo(DataTypes.StringType);
    }
    return names;
  }

  @Test
  public void documentStruct() {
    StructType t = CollectionPlan.documentStruct();
    assertThat(names(t)).containsExactly("key", "id", "rev", "doc").inOrder();
    assertThat(t.apply("key").nullable()).isFalse();
    assertThat(t.apply("id").nullable()).isTrue();
    assertThat(t.apply("rev").nullable()).isTrue();
    assertThat(t.apply("doc").nullable()).isFalse();
  }

  @Test
  public void edgeStruct() {
    StructType t = CollectionPlan.edgeStruct();
    assertThat(names(t)).containsExactly("key", "id", "rev", "from_id", "to_id", "doc").inOrder();
    assertThat(t.apply("from_id").nullable()).isFalse();
    assertThat(t.apply("to_id").nullable()).isFalse();
    assertThat(t.apply("doc").nullable()).isFalse();
  }

  private static void assertAvroMatchesStruct(String json, StructType struct, String recordName) {
    Schema s = new Schema.Parser().parse(json);
    assertThat(s.getName()).isEqualTo(recordName);
    assertThat(s.getNamespace()).isEqualTo("com.google.cloud.teleport.v2.tools.arangodb");
    List<Schema.Field> fields = s.getFields();
    assertThat(fields).hasSize(struct.fields().length);
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field f = fields.get(i);
      StructField sf = struct.fields()[i];
      assertThat(f.name()).isEqualTo(sf.name());
      if (sf.nullable()) {
        assertThat(f.schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(f.schema().getTypes().get(0).getType()).isEqualTo(Schema.Type.NULL);
        assertThat(f.schema().getTypes().get(1).getType()).isEqualTo(Schema.Type.STRING);
      } else {
        assertThat(f.schema().getType()).isEqualTo(Schema.Type.STRING);
      }
    }
  }

  @Test
  public void avroSchemasParseAndMatchStructs() {
    assertAvroMatchesStruct(
        CollectionPlan.documentAvroSchema(), CollectionPlan.documentStruct(), "ArangoDocumentRow");
    assertAvroMatchesStruct(
        CollectionPlan.edgeAvroSchema(), CollectionPlan.edgeStruct(), "ArangoEdgeRow");
  }

  @Test
  public void planUsesKindAndPartitions() {
    ExporterOptions opts =
        ExporterOptions.parse(
            new String[] {"--endpoints=h:1", "--output=/tmp/x", "--targetDocsPerPartition=10"});
    CollectionPlan p =
        CollectionPlan.create(new CollectionInfo("knows", CollectionKind.EDGE), "knows", 95, opts);
    assertThat(p.partitions()).isEqualTo(10);
    assertThat(p.kind()).isEqualTo(CollectionKind.EDGE);
    assertThat(p.tableName()).isEqualTo("knows");
    assertThat(p.sourceName()).isEqualTo("knows");
    assertThat(p.structType()).isEqualTo(CollectionPlan.edgeStruct());
    assertThat(p.avroSchemaJson()).isEqualTo(CollectionPlan.edgeAvroSchema());
    assertThat(p.columns()).hasSize(6);
    assertThat(p.columns().get(3).name).isEqualTo("from_id");
    assertThat(p.columns().get(5).type).isEqualTo("JSON");
    assertThat(p.columns().get(5).sourceName).isEqualTo("$");

    CollectionPlan d =
        CollectionPlan.create(
            new CollectionInfo("user-profiles", CollectionKind.DOCUMENT), "user_profiles", 1);
    assertThat(d.structType()).isEqualTo(CollectionPlan.documentStruct());
    assertThat(d.columns()).hasSize(4);
    assertThat(d.columns().get(0).nullable).isFalse();
    assertThat(d.toString()).contains("user_profiles");
  }
}
