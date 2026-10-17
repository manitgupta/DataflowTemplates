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
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsMappingsProvider}. */
@RunWith(JUnit4.class)
public class GcsMappingsProviderTest {

  private static Schema schemaOf(String type) {
    Schema union =
        GcsMappingsProvider.typeMapper()
            .getSchema(new SourceColumnType(type, new Long[] {}, new Long[] {}));
    assertThat(union.getType()).isEqualTo(Schema.Type.UNION);
    assertThat(union.getTypes().get(0).getType()).isEqualTo(Schema.Type.NULL);
    return union.getTypes().get(1);
  }

  @Test
  public void testMappingKeys() {
    assertThat(GcsMappingsProvider.getMapping().keySet())
        .containsExactlyElementsIn(GcsMappingsProvider.SUPPORTED_TYPES);
    assertThat(GcsMappingsProvider.SUPPORTED_TYPES)
        .containsExactly(
            "STRING", "JSON", "LONG", "DOUBLE", "BOOLEAN", "TIMESTAMP", "DATE", "BYTES");
  }

  @Test
  public void testAvroSchemas() {
    assertThat(schemaOf("STRING").getType()).isEqualTo(Schema.Type.STRING);
    assertThat(schemaOf("STRING").getProp("logicalType")).isNull();
    assertThat(schemaOf("JSON").getType()).isEqualTo(Schema.Type.STRING);
    assertThat(schemaOf("JSON").getProp("logicalType")).isEqualTo("json");
    assertThat(schemaOf("LONG").getType()).isEqualTo(Schema.Type.LONG);
    assertThat(schemaOf("DOUBLE").getType()).isEqualTo(Schema.Type.DOUBLE);
    assertThat(schemaOf("BOOLEAN").getType()).isEqualTo(Schema.Type.BOOLEAN);
    assertThat(schemaOf("TIMESTAMP").getLogicalType())
        .isInstanceOf(LogicalTypes.TimestampMicros.class);
    assertThat(schemaOf("DATE").getLogicalType()).isInstanceOf(LogicalTypes.Date.class);
    assertThat(schemaOf("BYTES").getType()).isEqualTo(Schema.Type.BYTES);
    // Case-insensitive lookup in the type mapper.
    assertThat(schemaOf("json").getProp("logicalType")).isEqualTo("json");
  }

  @Test
  public void testNullStaysNull() {
    for (String type : GcsMappingsProvider.SUPPORTED_TYPES) {
      assertThat(GcsMappingsProvider.toPayloadValue(type, null)).isNull();
    }
  }

  @Test
  public void testString() {
    assertThat(GcsMappingsProvider.toPayloadValue("STRING", new Utf8("abc"))).isEqualTo("abc");
    assertThat(GcsMappingsProvider.toPayloadValue("string", "abc")).isEqualTo("abc");
    assertThat(GcsMappingsProvider.toPayloadValue("STRING", 12L)).isEqualTo("12");
    assertThat(GcsMappingsProvider.toPayloadValue("STRING", 1.5d)).isEqualTo("1.5");
    assertThat(GcsMappingsProvider.toPayloadValue("STRING", true)).isEqualTo("true");
    ByteBuffer bb = ByteBuffer.wrap("h\u00e9".getBytes(StandardCharsets.UTF_8));
    assertThat(GcsMappingsProvider.toPayloadValue("STRING", bb)).isEqualTo("h\u00e9");
    assertThat(bb.remaining()).isEqualTo(3);
  }

  @Test
  public void testJson() {
    assertThat(GcsMappingsProvider.toPayloadValue("JSON", new Utf8("{\"a\":1}")))
        .isEqualTo("{\"a\":1}");
    // Invalid JSON is passed through; Spanner rejects it later.
    assertThat(GcsMappingsProvider.toPayloadValue("JSON", "{bad")).isEqualTo("{bad");
    assertThat(
            GcsMappingsProvider.toPayloadValue(
                "JSON", ByteBuffer.wrap("[1]".getBytes(StandardCharsets.UTF_8))))
        .isEqualTo("[1]");
  }

  @Test
  public void testLong() {
    assertThat(GcsMappingsProvider.toPayloadValue("LONG", 5)).isEqualTo(5L);
    assertThat(GcsMappingsProvider.toPayloadValue("LONG", 9223372036854775807L))
        .isEqualTo(9223372036854775807L);
    assertThat(GcsMappingsProvider.toPayloadValue("LONG", new Utf8(" -42 "))).isEqualTo(-42L);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsMappingsProvider.toPayloadValue("LONG", new Utf8("abc")));
    assertThat(e).hasMessageThat().contains("LONG");
    assertThat(e).hasMessageThat().contains("Utf8");
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("LONG", 1.5d));
  }

  @Test
  public void testDouble() {
    assertThat(GcsMappingsProvider.toPayloadValue("DOUBLE", 3)).isEqualTo(3.0d);
    assertThat(GcsMappingsProvider.toPayloadValue("DOUBLE", 1.5f)).isEqualTo(1.5d);
    assertThat(GcsMappingsProvider.toPayloadValue("DOUBLE", "1e300")).isEqualTo(1e300d);
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("DOUBLE", "x"));
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("DOUBLE", true));
  }

  @Test
  public void testBoolean() {
    assertThat(GcsMappingsProvider.toPayloadValue("BOOLEAN", true)).isEqualTo(true);
    assertThat(GcsMappingsProvider.toPayloadValue("BOOLEAN", new Utf8("TRUE"))).isEqualTo(true);
    assertThat(GcsMappingsProvider.toPayloadValue("BOOLEAN", "false")).isEqualTo(false);
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("BOOLEAN", "yes"));
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("BOOLEAN", 1));
  }

  @Test
  public void testTimestamp() {
    assertThat(GcsMappingsProvider.toPayloadValue("TIMESTAMP", 123L)).isEqualTo(123L);
    long expected = Instant.parse("2026-10-17T10:00:00Z").toEpochMilli() * 1000L + 1;
    assertThat(GcsMappingsProvider.toPayloadValue("TIMESTAMP", "2026-10-17T10:00:00.000001Z"))
        .isEqualTo(expected);
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsMappingsProvider.toPayloadValue("TIMESTAMP", "2026-10-17"));
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("TIMESTAMP", 1));
  }

  @Test
  public void testDate() {
    assertThat(GcsMappingsProvider.toPayloadValue("DATE", 7)).isEqualTo(7);
    assertThat(GcsMappingsProvider.toPayloadValue("DATE", new Utf8("2026-10-17")))
        .isEqualTo((int) LocalDate.of(2026, 10, 17).toEpochDay());
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsMappingsProvider.toPayloadValue("DATE", "17/10/2026"));
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("DATE", 7L));
  }

  @Test
  public void testBytes() {
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {1, 2});
    assertThat(GcsMappingsProvider.toPayloadValue("BYTES", bb)).isSameInstanceAs(bb);
    assertThat(GcsMappingsProvider.toPayloadValue("BYTES", new byte[] {3}))
        .isEqualTo(ByteBuffer.wrap(new byte[] {3}));
    Schema fixed = Schema.createFixed("f", null, null, 1);
    assertThat(
            GcsMappingsProvider.toPayloadValue(
                "BYTES", new GenericData.Fixed(fixed, new byte[] {4})))
        .isEqualTo(ByteBuffer.wrap(new byte[] {4}));
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("BYTES", "abc"));
  }

  @Test
  public void testUnknownTypeThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> GcsMappingsProvider.toPayloadValue("FOO", "x"));
  }
}
