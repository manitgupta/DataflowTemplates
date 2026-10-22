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
package com.google.cloud.teleport.v2.templates.debezium;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventTypeConvertor;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DebeziumChangeEventMapper}. */
@RunWith(JUnit4.class)
public class DebeziumChangeEventMapperTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private final DebeziumChangeEventMapper mapper = new DebeziumChangeEventMapper("mysql");

  private static final String COLUMNS =
      "["
          + "{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},"
          + "{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},"
          + "{\"type\":\"bytes\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Decimal\","
          + "\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"12\"},\"field\":\"balance\"},"
          + "{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"field\":\"birth\"},"
          + "{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTimestamp\","
          + "\"field\":\"created_at\"},"
          + "{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.Timestamp\","
          + "\"field\":\"created_ms\"},"
          + "{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.time.ZonedTimestamp\","
          + "\"field\":\"updated_at\"},"
          + "{\"type\":\"bytes\",\"optional\":true,\"field\":\"digest\"},"
          + "{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Json\",\"field\":\"prefs\"},"
          + "{\"type\":\"int16\",\"optional\":true,\"field\":\"is_active\"},"
          + "{\"type\":\"int64\",\"optional\":true,\"name\":\"io.debezium.time.MicroTime\",\"field\":\"t\"},"
          + "{\"type\":\"bytes\",\"optional\":true,\"name\":\"io.debezium.data.Bits\","
          + "\"parameters\":{\"length\":\"16\"},\"field\":\"flags\"},"
          + "{\"type\":\"double\",\"optional\":true,\"field\":\"score\"}"
          + "]";

  private static String row(long id, String name) {
    // balance 1234.56 -> unscaled 123456 = 0x01E240
    String balance = Base64.getEncoder().encodeToString(new byte[] {0x01, (byte) 0xE2, 0x40});
    String digest = Base64.getEncoder().encodeToString(new byte[] {0x00, (byte) 0xAB, 0x10});
    // BIT(16) value 0x0102 in little-endian order
    String flags = Base64.getEncoder().encodeToString(new byte[] {0x02, 0x01});
    return "{\"id\":"
        + id
        + ",\"name\":\""
        + name
        + "\",\"balance\":\""
        + balance
        + "\",\"birth\":20000,\"created_at\":1761134400123456,\"created_ms\":1761134400123,"
        + "\"updated_at\":\"2025-10-22T12:00:00.654321Z\",\"digest\":\""
        + digest
        + "\",\"prefs\":\"{\\\"a\\\":1}\",\"is_active\":1,\"t\":3723000004,\"flags\":\""
        + flags
        + "\",\"score\":1.5}";
  }

  private static String source(boolean snapshot, String file, long pos, int row) {
    return "{\"version\":\"3.0.0.Final\",\"connector\":\"mysql\",\"name\":\"shard1\","
        + "\"ts_ms\":1761134401000,\"snapshot\":\""
        + (snapshot ? "true" : "false")
        + "\",\"db\":\"shop\",\"table\":\"customers\",\"server_id\":1,\"file\":\""
        + file
        + "\",\"pos\":"
        + pos
        + ",\"row\":"
        + row
        + "}";
  }

  private static byte[] envelope(String op, String before, String after, String source) {
    String schema =
        "{\"type\":\"struct\",\"name\":\"shard1.shop.customers.Envelope\",\"fields\":["
            + "{\"type\":\"struct\",\"optional\":true,\"field\":\"before\",\"fields\":"
            + COLUMNS
            + "},"
            + "{\"type\":\"struct\",\"optional\":true,\"field\":\"after\",\"fields\":"
            + COLUMNS
            + "},"
            + "{\"type\":\"struct\",\"field\":\"source\",\"fields\":[]},"
            + "{\"type\":\"string\",\"field\":\"op\"},"
            + "{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}]}";
    String payload =
        "{\"before\":"
            + before
            + ",\"after\":"
            + after
            + ",\"source\":"
            + source
            + ",\"op\":\""
            + op
            + "\",\"ts_ms\":1761134402500}";
    return ("{\"schema\":" + schema + ",\"payload\":" + payload + "}")
        .getBytes(StandardCharsets.UTF_8);
  }

  private static final byte[] KEY =
      "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"field\":\"id\"}]},\"payload\":{\"id\":7}}"
          .getBytes(StandardCharsets.UTF_8);

  private JsonNode mapChange(byte[] value) throws IOException {
    DebeziumChangeEventMapper.Result r = mapper.map(KEY, value, "t/0/1");
    assertThat(r.kind()).isEqualTo(DebeziumChangeEventMapper.Kind.CHANGE);
    // Round-trip through a string exactly like the pipeline does.
    return JSON.readTree(r.event().toString());
  }

  @Test
  public void insertMapsMetadataAndValues() throws Exception {
    JsonNode e =
        mapChange(envelope("c", "null", row(7, "ann"), source(false, "mysql-bin.000003", 4567, 2)));
    assertThat(e.get("_metadata_change_type").asText()).isEqualTo("INSERT");
    assertThat(e.get("_metadata_source_type").asText()).isEqualTo("mysql");
    assertThat(e.get("_metadata_read_method").asText()).isEqualTo("mysql-cdc-binlog");
    assertThat(e.get("_metadata_stream").asText()).isEqualTo("shard1");
    assertThat(e.get("_metadata_schema").asText()).isEqualTo("shop");
    assertThat(e.get("_metadata_table").asText()).isEqualTo("customers");
    assertThat(e.get("_metadata_deleted").asBoolean()).isFalse();
    assertThat(e.get("_metadata_timestamp").asLong()).isEqualTo(1761134401L);
    assertThat(e.get("_metadata_timestamp_ms").asLong()).isEqualTo(1761134401000L);
    assertThat(e.get("_metadata_read_timestamp").asLong()).isEqualTo(1761134402L);
    assertThat(e.get("_metadata_log_file").asText()).isEqualTo("mysql-bin.000003");
    assertThat(e.get("_metadata_log_position").asLong()).isEqualTo((4567L << 16) + 2);
    assertThat(e.get("_metadata_primary_keys").get(0).asText()).isEqualTo("id");
    assertThat(e.get("_metadata_uuid").asText()).isNotEmpty();
    assertThat(e.get("id").asLong()).isEqualTo(7);
    assertThat(e.get("name").asText()).isEqualTo("ann");
    assertThat(e.get("is_active").asInt()).isEqualTo(1);
    assertThat(e.get("prefs").asText()).isEqualTo("{\"a\":1}");
    assertThat(e.get("t").asText()).isEqualTo("01:02:03.000004");
    assertThat(e.get("flags").asLong()).isEqualTo(0x0102);
    assertThat(e.get("score").asDouble()).isEqualTo(1.5);
  }

  @Test
  public void valuesAreReadableByTheTemplateTypeConvertor() throws Exception {
    JsonNode e =
        mapChange(envelope("c", "null", row(7, "ann"), source(false, "mysql-bin.000003", 1, 0)));
    assertThat(ChangeEventTypeConvertor.toNumericBigDecimal(e, "balance", true))
        .isEqualTo(new BigDecimal("1234.560000000"));
    assertThat(ChangeEventTypeConvertor.toDate(e, "birth", true))
        .isEqualTo(Date.fromYearMonthDay(2024, 10, 4));
    assertThat(ChangeEventTypeConvertor.toTimestamp(e, "created_at", true))
        .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1761134400L, 123456000));
    assertThat(ChangeEventTypeConvertor.toTimestamp(e, "created_ms", true))
        .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1761134400L, 123000000));
    assertThat(ChangeEventTypeConvertor.toTimestamp(e, "updated_at", true))
        .isEqualTo(Timestamp.ofTimeSecondsAndNanos(1761134400L, 654321000));
    assertThat(ChangeEventTypeConvertor.toByteArray(e, "digest", true))
        .isEqualTo(ByteArray.copyFrom(new byte[] {0x00, (byte) 0xAB, 0x10}));
    assertThat(ChangeEventTypeConvertor.toLong(e, "_metadata_timestamp", true))
        .isEqualTo(1761134401L);
  }

  @Test
  public void updateUsesAfterImage() throws Exception {
    JsonNode e =
        mapChange(
            envelope("u", row(7, "old"), row(7, "new"), source(false, "mysql-bin.000003", 99, 0)));
    assertThat(e.get("_metadata_change_type").asText()).isEqualTo("UPDATE");
    assertThat(e.get("name").asText()).isEqualTo("new");
  }

  @Test
  public void deleteUsesBeforeImage() throws Exception {
    JsonNode e =
        mapChange(envelope("d", row(7, "gone"), "null", source(false, "mysql-bin.000004", 5, 0)));
    assertThat(e.get("_metadata_change_type").asText()).isEqualTo("DELETE");
    assertThat(e.get("_metadata_deleted").asBoolean()).isTrue();
    assertThat(e.get("name").asText()).isEqualTo("gone");
    assertThat(e.get("id").asLong()).isEqualTo(7);
  }

  @Test
  public void snapshotReadIsMappedAsBackfill() throws Exception {
    JsonNode e =
        mapChange(envelope("r", "null", row(7, "ann"), source(true, "mysql-bin.000003", 154, 0)));
    assertThat(e.get("_metadata_change_type").asText()).isEqualTo("INSERT");
    assertThat(e.get("_metadata_read_method").asText()).isEqualTo("mysql-backfill-fulldump");
    assertThat(e.get("_metadata_log_file").isNull()).isTrue();
    assertThat(e.get("_metadata_log_position").isNull()).isTrue();
  }

  @Test
  public void ignoresTombstonesTruncatesAndNonRowRecords() throws Exception {
    assertThat(mapper.map(KEY, null, "s").kind()).isEqualTo(DebeziumChangeEventMapper.Kind.IGNORED);
    assertThat(mapper.map(KEY, "null".getBytes(StandardCharsets.UTF_8), "s").kind())
        .isEqualTo(DebeziumChangeEventMapper.Kind.IGNORED);
    assertThat(
            mapper
                .map(
                    KEY,
                    envelope("t", "null", "null", source(false, "mysql-bin.000001", 1, 0)),
                    "s")
                .reason())
        .isEqualTo("truncate");
    byte[] schemaChange =
        "{\"source\":{\"name\":\"shard1\"},\"databaseName\":\"shop\",\"ddl\":\"ALTER TABLE x\"}"
            .getBytes(StandardCharsets.UTF_8);
    assertThat(mapper.map(null, schemaChange, "s").kind())
        .isEqualTo(DebeziumChangeEventMapper.Kind.IGNORED);
  }

  @Test
  public void schemalessEventsPassValuesThrough() throws Exception {
    byte[] value =
        ("{\"before\":null,\"after\":{\"id\":3,\"name\":\"x\",\"amount\":\"12.50\"},"
                + "\"source\":"
                + source(false, "mysql-bin.000010", 7, 1)
                + ",\"op\":\"c\",\"ts_ms\":1}")
            .getBytes(StandardCharsets.UTF_8);
    JsonNode e = mapChange(value);
    assertThat(e.get("id").asLong()).isEqualTo(3);
    assertThat(e.get("amount").asText()).isEqualTo("12.50");
    assertThat(e.get("_metadata_log_position").asLong()).isEqualTo((7L << 16) + 1);
  }

  @Test
  public void malformedRecordsThrow() {
    assertThrows(
        IOException.class, () -> mapper.map(KEY, "not json".getBytes(StandardCharsets.UTF_8), "s"));
    assertThrows(
        IOException.class,
        () ->
            mapper.map(
                KEY, "{\"op\":\"c\",\"after\":{\"id\":1}}".getBytes(StandardCharsets.UTF_8), "s"));
    assertThrows(
        IOException.class,
        () ->
            mapper.map(
                KEY, envelope("u", "null", "null", source(false, "mysql-bin.000001", 1, 0)), "s"));
  }

  @Test
  public void logPositionOrdersByPositionThenRow() {
    assertThat(DebeziumChangeEventMapper.logPosition(100, 5))
        .isLessThan(DebeziumChangeEventMapper.logPosition(101, 0));
    assertThat(DebeziumChangeEventMapper.logPosition(100, 0))
        .isLessThan(DebeziumChangeEventMapper.logPosition(100, 1));
    // max binlog size is 1 GiB, so pos << 16 never overflows
    assertThat(DebeziumChangeEventMapper.logPosition(1L << 30, 65535)).isGreaterThan(0L);
  }

  @Test
  public void formatsMySqlTimeDurations() {
    assertThat(DebeziumChangeEventMapper.formatTime(0)).isEqualTo("00:00:00");
    assertThat(DebeziumChangeEventMapper.formatTime(-3_600_000_000L)).isEqualTo("-01:00:00");
    assertThat(DebeziumChangeEventMapper.formatTime(90_000_000_000L + 5))
        .isEqualTo("25:00:00.000005");
  }

  @Test
  public void rejectsUnsupportedSources() {
    assertThrows(IllegalArgumentException.class, () -> new DebeziumChangeEventMapper("oracle"));
  }

  @Test
  public void parsesConsumerConfig() {
    Map<String, Object> props =
        ReadDebeziumChangeEvents.parseConsumerConfig(
            "security.protocol=SASL_SSL; sasl.jaas.config=a=b c=d ;");
    assertThat(props).containsEntry("security.protocol", "SASL_SSL");
    assertThat(props).containsEntry("sasl.jaas.config", "a=b c=d");
    assertThrows(
        IllegalArgumentException.class, () -> ReadDebeziumChangeEvents.parseConsumerConfig("x"));
  }
}
