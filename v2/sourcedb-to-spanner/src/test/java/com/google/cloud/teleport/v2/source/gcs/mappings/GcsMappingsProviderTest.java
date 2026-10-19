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
package com.google.cloud.teleport.v2.source.gcs.mappings;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.nio.ByteBuffer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsMappingsProvider}. */
@RunWith(JUnit4.class)
public class GcsMappingsProviderTest {

  private static Schema nullable(Schema s) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), s);
  }

  private static Schema json() {
    Schema s = Schema.create(Schema.Type.STRING);
    s.addProp("logicalType", "json");
    return s;
  }

  private static Schema typeOf(String name) {
    return GcsMappingsProvider.typeMapper()
        .getSchema(new SourceColumnType(name, new Long[] {}, new Long[] {}));
  }

  @Test
  public void testTypeMapperMatchesDerivedNames() {
    // The unified schema of each mapped type name derives that name back.
    for (String name : GcsMappingsProvider.getMapping().keySet()) {
      assertThat(GcsMappingsProvider.columnTypeName(typeOf(name))).isEqualTo(name);
    }
  }

  @Test
  public void testColumnTypeName() {
    assertThat(GcsMappingsProvider.columnTypeName(nullable(Schema.create(Schema.Type.STRING))))
        .isEqualTo("STRING");
    assertThat(GcsMappingsProvider.columnTypeName(nullable(json()))).isEqualTo("JSON");
    assertThat(GcsMappingsProvider.columnTypeName(Schema.create(Schema.Type.INT)))
        .isEqualTo("INTEGER");
    assertThat(
            GcsMappingsProvider.columnTypeName(
                LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES))))
        .isEqualTo("DECIMAL");
    assertThat(
            GcsMappingsProvider.columnTypeName(
                nullable(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))))
        .isEqualTo("DATE");
    Schema custom = Schema.create(Schema.Type.STRING);
    custom.addProp("logicalType", "varchar");
    assertThat(GcsMappingsProvider.columnTypeName(custom)).isEqualTo("VARCHAR");
    assertThat(
            GcsMappingsProvider.columnTypeName(
                SchemaBuilder.record("r").fields().requiredLong("a").endRecord()))
        .isEqualTo("RECORD");
    assertThat(
            GcsMappingsProvider.columnTypeName(
                SchemaBuilder.unionOf().nullType().and().stringType().and().longType().endUnion()))
        .isEqualTo("UNION");
    assertThat(GcsMappingsProvider.columnType(nullable(json())).getName()).isEqualTo("JSON");
  }

  @Test
  public void testIsJsonAndIsString() {
    assertThat(GcsMappingsProvider.isJson(nullable(json()))).isTrue();
    assertThat(GcsMappingsProvider.isJson(json())).isTrue();
    assertThat(GcsMappingsProvider.isJson(nullable(Schema.create(Schema.Type.STRING)))).isFalse();
    assertThat(GcsMappingsProvider.isJson(typeOf("JSON"))).isTrue();
    assertThat(GcsMappingsProvider.isString(nullable(Schema.create(Schema.Type.STRING)))).isTrue();
    assertThat(GcsMappingsProvider.isString(Schema.create(Schema.Type.BYTES))).isFalse();
  }

  @Test
  public void testToPayloadValue() {
    Schema str = nullable(Schema.create(Schema.Type.STRING));
    assertThat(GcsMappingsProvider.toPayloadValue(str, null)).isNull();
    Object s = GcsMappingsProvider.toPayloadValue(str, new Utf8("abc"));
    assertThat(s).isInstanceOf(String.class);
    assertThat(s).isEqualTo("abc");

    // JSON numbers are canonicalized; invalid JSON passes through (Spanner rejects it later).
    assertThat(GcsMappingsProvider.toPayloadValue(nullable(json()), new Utf8("{\"a\":1.50}")))
        .isEqualTo("{\"a\":1.5}");
    assertThat(GcsMappingsProvider.toPayloadValue(nullable(json()), "{bad")).isEqualTo("{bad");

    Object l = 5L;
    assertThat(GcsMappingsProvider.toPayloadValue(Schema.create(Schema.Type.LONG), l))
        .isSameInstanceAs(l);
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {1, 2});
    assertThat(GcsMappingsProvider.toPayloadValue(Schema.create(Schema.Type.BYTES), bb))
        .isSameInstanceAs(bb);
    // A string value of a non-string field (e.g. an enum symbol in a union) is left as is.
    Utf8 u = new Utf8("x");
    assertThat(GcsMappingsProvider.toPayloadValue(Schema.create(Schema.Type.BYTES), u))
        .isSameInstanceAs(u);
  }
}
