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

  static final String DOCUMENT_SCHEMA_TEXT =
      "{\"type\":\"record\",\"name\":\"SourceRowWithMetadata\",\"fields\":["
          + "{\"name\":\"tableName\",\"type\":\"string\"},"
          + "{\"name\":\"shardId\",\"type\":[\"string\",\"null\"]},"
          + "{\"name\":\"primaryKeys\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
          + "{\"name\":\"payload\",\"type\":{\"type\":\"record\",\"name\":\"payload\",\"fields\":["
          + "{\"name\":\"key\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"id\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"rev\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"doc\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"json\"}]}"
          + "]}}]}";

  private static void assertBulkSchema(String json, StructType struct) {
    Schema s = new Schema.Parser().parse(json);
    assertThat(s.toString()).isEqualTo(json);
    assertThat(s.getName()).isEqualTo("SourceRowWithMetadata");
    assertThat(s.getField("tableName").schema().getType()).isEqualTo(Schema.Type.STRING);
    assertThat(s.getField("shardId").schema().getTypes().get(1).getType())
        .isEqualTo(Schema.Type.NULL);
    assertThat(s.getField("primaryKeys").schema().getType()).isEqualTo(Schema.Type.ARRAY);
    List<Schema.Field> fields = s.getField("payload").schema().getFields();
    assertThat(fields).hasSize(struct.fields().length);
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field f = fields.get(i);
      assertThat(f.name()).isEqualTo(struct.fields()[i].name());
      assertThat(f.schema().getType()).isEqualTo(Schema.Type.UNION);
      assertThat(f.schema().getTypes().get(0).getType()).isEqualTo(Schema.Type.NULL);
      Schema value = f.schema().getTypes().get(1);
      assertThat(value.getType()).isEqualTo(Schema.Type.STRING);
      assertThat(value.getProp("logicalType")).isEqualTo(f.name().equals("doc") ? "json" : null);
    }
  }

  @Test
  public void bulkSchemasAreTheGoldenResources() {
    assertThat(CollectionPlan.documentAvroSchema()).isEqualTo(DOCUMENT_SCHEMA_TEXT);
    assertThat(CollectionPlan.edgeAvroSchema())
        .isEqualTo(
            DOCUMENT_SCHEMA_TEXT.replace(
                "{\"name\":\"doc\"",
                "{\"name\":\"from_id\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"to_id\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"doc\""));
    assertBulkSchema(CollectionPlan.documentAvroSchema(), CollectionPlan.documentStruct());
    assertBulkSchema(CollectionPlan.edgeAvroSchema(), CollectionPlan.edgeStruct());
  }

  @Test
  public void schemaMd5NamesTheFiles() {
    CollectionPlan doc =
        CollectionPlan.create(new CollectionInfo("users", CollectionKind.DOCUMENT), "users", 1);
    CollectionPlan edge =
        CollectionPlan.create(new CollectionInfo("knows", CollectionKind.EDGE), "knows", 1);
    assertThat(doc.avroSchemaMd5()).isEqualTo("b36539a80425c7f2c6178856a7bb935f");
    assertThat(edge.avroSchemaMd5()).isEqualTo("a7f80905c6bc6e9912edef8c18d80a52");
    assertThat(CollectionPlan.md5Hex("")).isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
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
    assertThat(p.avroSchemaMd5()).isEqualTo(CollectionPlan.md5Hex(CollectionPlan.edgeAvroSchema()));
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
