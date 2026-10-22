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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.source.mysql.MySqlDsToSpSourceConnector;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Maps a Debezium change event (Kafka Connect JSON converter, with or without the schema envelope)
 * to the Datastream-shaped JSON record that the rest of the pipeline consumes.
 *
 * <p>Only the reader changes: the output carries the same {@code _metadata_*} fields that {@code
 * FormatDatastreamJsonToJson} emits, and column values are encoded the way Datastream encodes them
 * (ISO-8601 dates and timestamps, hex for binary, plain strings for decimals), so ordering via
 * shadow tables, transformations, sharding and the dead-letter queue work unchanged.
 *
 * <p>MySQL ordering uses {@code (log_file, log_position)}. Debezium's {@code source.pos} is the
 * binlog position of the row event and {@code source.row} is the row index inside that event; both
 * are folded into {@code _metadata_log_position} as {@code pos * 2^16 + row}. Snapshot reads
 * ({@code op=r}) are mapped like Datastream backfill events (no log file), so any streamed change
 * for the same key wins over them.
 */
public class DebeziumChangeEventMapper implements Serializable {

  /** Read method values Datastream uses for MySQL; the template only inspects the prefix. */
  public static final String MYSQL_CDC_READ_METHOD = "mysql-cdc-binlog";

  public static final String MYSQL_BACKFILL_READ_METHOD = "mysql-backfill-fulldump";

  /** Rows per binlog row event are far below 2^16 (events are capped at a few KB by default). */
  static final int ROW_BITS = 16;

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS").withZone(ZoneOffset.UTC);

  /** Outcome of mapping one Kafka record. */
  public enum Kind {
    /** A row change, see {@link Result#event()}. */
    CHANGE,
    /** A record the pipeline does not apply: tombstone, truncate, heartbeat, schema change. */
    IGNORED
  }

  /** Mapping result: a Datastream-shaped event or the reason the record was ignored. */
  public static final class Result {
    private final Kind kind;
    private final ObjectNode event;
    private final String reason;

    private Result(Kind kind, ObjectNode event, String reason) {
      this.kind = kind;
      this.event = event;
      this.reason = reason;
    }

    static Result change(ObjectNode event) {
      return new Result(Kind.CHANGE, event, null);
    }

    static Result ignored(String reason) {
      return new Result(Kind.IGNORED, null, reason);
    }

    public Kind kind() {
      return kind;
    }

    public ObjectNode event() {
      return event;
    }

    public String reason() {
      return reason;
    }
  }

  private final String sourceType;
  private transient ObjectMapper mapper;

  public DebeziumChangeEventMapper(String sourceType) {
    if (!"mysql".equalsIgnoreCase(sourceType)) {
      throw new IllegalArgumentException(
          "Debezium CDC currently supports MySQL sources only, got: " + sourceType);
    }
    this.sourceType = sourceType.toLowerCase();
  }

  private ObjectMapper mapper() {
    if (mapper == null) {
      mapper = new ObjectMapper();
    }
    return mapper;
  }

  /**
   * Maps one Kafka record.
   *
   * @param key the record key bytes (may be null)
   * @param value the record value bytes (null for tombstones)
   * @param uuidSeed a value that uniquely and stably identifies the record, e.g.
   *     topic/partition/offset; used for {@code _metadata_uuid}
   * @throws IOException if the value is not valid JSON or not a Debezium change event
   */
  public Result map(byte[] key, byte[] value, String uuidSeed) throws IOException {
    if (value == null || value.length == 0) {
      return Result.ignored("tombstone");
    }
    JsonNode root = mapper().readTree(value);
    if (root == null || root.isNull()) {
      return Result.ignored("tombstone");
    }
    JsonNode payload = root;
    Map<String, JsonNode> columnSchemas = new HashMap<>();
    if (root.has("payload") && root.has("schema")) {
      payload = root.get("payload");
      if (payload == null || payload.isNull()) {
        return Result.ignored("tombstone");
      }
      collectColumnSchemas(root.get("schema"), columnSchemas);
    }
    JsonNode opNode = payload.get("op");
    if (opNode == null || opNode.isNull()) {
      // Schema change, heartbeat or transaction metadata records carry no row operation.
      return Result.ignored("no-op-field");
    }
    String op = opNode.asText();
    JsonNode source = payload.get("source");
    if (source == null || !source.isObject()) {
      throw new IOException("Debezium change event has no source block");
    }

    String changeType;
    JsonNode row;
    switch (op) {
      case "c":
      case "r":
        changeType = DatastreamConstants.INSERT_EVENT;
        row = payload.get("after");
        break;
      case "u":
        changeType = DatastreamConstants.UPDATE_EVENT;
        row = payload.get("after");
        break;
      case "d":
        changeType = DatastreamConstants.DELETE_EVENT;
        row = payload.get("before");
        break;
      case "t":
        return Result.ignored("truncate");
      case "m":
        return Result.ignored("message");
      default:
        throw new IOException("Unknown Debezium operation: " + op);
    }
    if (row == null || !row.isObject()) {
      throw new IOException(
          "Debezium '" + op + "' event has no row image; is binlog_row_image=FULL set?");
    }

    boolean snapshot = "r".equals(op);
    long sourceTsMs = source.path("ts_ms").asLong(payload.path("ts_ms").asLong(0));
    long readTsMs = payload.path("ts_ms").asLong(sourceTsMs);

    ObjectNode out = JsonNodeFactory.instance.objectNode();
    out.put(DatastreamConstants.EVENT_STREAM_NAME, source.path("name").asText(""));
    out.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, sourceTsMs / 1000L);
    out.put("_metadata_timestamp_ms", sourceTsMs);
    out.put("_metadata_read_timestamp", readTsMs / 1000L);
    out.put("_metadata_dataflow_timestamp", System.currentTimeMillis() / 1000L);
    out.put(
        DatastreamConstants.EVENT_READ_METHOD_KEY,
        snapshot ? MYSQL_BACKFILL_READ_METHOD : MYSQL_CDC_READ_METHOD);
    out.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, sourceType);
    out.put("_metadata_deleted", "d".equals(op));
    String db = textOrNull(source.get("db"));
    out.put("_metadata_database", db);
    out.put(DatastreamConstants.EVENT_SCHEMA_KEY, db);
    out.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, textOrNull(source.get("table")));
    out.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, changeType);
    out.set("_metadata_primary_keys", primaryKeys(key));
    out.put(
        DatastreamConstants.EVENT_UUID_KEY,
        UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8)).toString());
    if (snapshot) {
      out.putNull(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY);
      out.putNull(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY);
    } else {
      String file = textOrNull(source.get("file"));
      if (file == null || !source.hasNonNull("pos")) {
        throw new IOException("Debezium MySQL event has no binlog file/position");
      }
      out.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, file);
      out.put(
          MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY,
          logPosition(source.get("pos").asLong(), source.path("row").asInt(0)));
    }

    Iterator<Map.Entry<String, JsonNode>> fields = row.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      out.set(field.getKey(), convert(field.getValue(), columnSchemas.get(field.getKey())));
    }
    out.set("_metadata_source", source);
    return Result.change(out);
  }

  static long logPosition(long pos, int row) {
    return (pos << ROW_BITS) + Math.min(Math.max(row, 0), (1 << ROW_BITS) - 1);
  }

  private static String textOrNull(JsonNode node) {
    return node == null || node.isNull() ? null : node.asText();
  }

  private JsonNode primaryKeys(byte[] key) {
    if (key == null || key.length == 0) {
      return JsonNodeFactory.instance.nullNode();
    }
    try {
      JsonNode keyNode = mapper().readTree(key);
      if (keyNode != null && keyNode.has("payload") && keyNode.has("schema")) {
        keyNode = keyNode.get("payload");
      }
      if (keyNode == null || !keyNode.isObject()) {
        return JsonNodeFactory.instance.nullNode();
      }
      com.fasterxml.jackson.databind.node.ArrayNode names = JsonNodeFactory.instance.arrayNode();
      keyNode.fieldNames().forEachRemaining(names::add);
      return names;
    } catch (IOException e) {
      return JsonNodeFactory.instance.nullNode();
    }
  }

  /** Collects the column schemas of the {@code after} (or {@code before}) struct. */
  private static void collectColumnSchemas(JsonNode envelopeSchema, Map<String, JsonNode> out) {
    if (envelopeSchema == null || !envelopeSchema.has("fields")) {
      return;
    }
    for (JsonNode f : envelopeSchema.get("fields")) {
      String name = f.path("field").asText();
      if (("after".equals(name) || "before".equals(name)) && f.has("fields")) {
        for (JsonNode col : f.get("fields")) {
          out.putIfAbsent(col.path("field").asText(), col);
        }
      }
    }
  }

  /**
   * Converts one Debezium column value to the Datastream JSON encoding using its Connect schema.
   * Values without a schema (schemas.enable=false) are passed through unchanged.
   */
  static JsonNode convert(JsonNode value, JsonNode schema) {
    if (value == null || value.isNull() || schema == null) {
      return value == null ? JsonNodeFactory.instance.nullNode() : value;
    }
    JsonNodeFactory f = JsonNodeFactory.instance;
    String logical = schema.path("name").asText("");
    String type = schema.path("type").asText("");
    switch (logical) {
      case "io.debezium.time.Date":
        return f.textNode(LocalDate.ofEpochDay(value.asLong()).toString());
      case "io.debezium.time.Timestamp":
        return f.textNode(formatTimestamp(Instant.ofEpochMilli(value.asLong())));
      case "io.debezium.time.MicroTimestamp":
        return f.textNode(formatTimestamp(micros(value.asLong())));
      case "io.debezium.time.NanoTimestamp":
        return f.textNode(
            formatTimestamp(
                Instant.ofEpochSecond(
                    Math.floorDiv(value.asLong(), 1_000_000_000L),
                    Math.floorMod(value.asLong(), 1_000_000_000L))));
      case "io.debezium.time.ZonedTimestamp":
      case "io.debezium.time.ZonedTime":
      case "io.debezium.data.Json":
      case "io.debezium.data.Enum":
      case "io.debezium.data.EnumSet":
        return value;
      case "io.debezium.time.Time":
        return f.textNode(formatTime(value.asLong() * 1000L));
      case "io.debezium.time.MicroTime":
        return f.textNode(formatTime(value.asLong()));
      case "io.debezium.time.NanoTime":
        return f.textNode(formatTime(value.asLong() / 1000L));
      case "org.apache.kafka.connect.data.Decimal":
        if (value.isTextual() && "bytes".equals(type)) {
          int scale = Integer.parseInt(schema.path("parameters").path("scale").asText("0"));
          BigDecimal d =
              new BigDecimal(new BigInteger(Base64.getDecoder().decode(value.asText())), scale);
          return f.textNode(d.toPlainString());
        }
        return value;
      case "io.debezium.data.VariableScaleDecimal":
        if (value.isObject()) {
          BigDecimal d =
              new BigDecimal(
                  new BigInteger(Base64.getDecoder().decode(value.path("value").asText())),
                  value.path("scale").asInt());
          return f.textNode(d.toPlainString());
        }
        return value;
      case "io.debezium.data.Bits":
        if (value.isTextual()) {
          // Debezium emits BIT(n) as little-endian bytes; Datastream emits the integer value.
          byte[] le = Base64.getDecoder().decode(value.asText());
          byte[] be = new byte[le.length];
          for (int i = 0; i < le.length; i++) {
            be[i] = le[le.length - 1 - i];
          }
          return f.numberNode(new BigInteger(1, be));
        }
        return value;
      default:
        break;
    }
    if ("bytes".equals(type) && value.isTextual()) {
      // Kafka Connect JSON encodes bytes as base64; Datastream (MySQL) encodes them as hex.
      return f.textNode(toHex(Base64.getDecoder().decode(value.asText())));
    }
    return value;
  }

  private static Instant micros(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
  }

  private static String formatTimestamp(Instant instant) {
    return TIMESTAMP_FORMATTER.format(instant) + "Z";
  }

  /** MySQL TIME is a duration that may be negative or exceed 24h, so format it by hand. */
  static String formatTime(long micros) {
    String sign = micros < 0 ? "-" : "";
    long abs = Math.abs(micros);
    long seconds = abs / 1_000_000L;
    long fraction = abs % 1_000_000L;
    String base =
        String.format("%s%02d:%02d:%02d", sign, seconds / 3600, (seconds / 60) % 60, seconds % 60);
    return fraction == 0 ? base : base + String.format(".%06d", fraction);
  }

  private static String toHex(byte[] bytes) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      out[2 * i] = digits[(bytes[i] >> 4) & 0xF];
      out[2 * i + 1] = digits[bytes[i] & 0xF];
    }
    return new String(out);
  }
}
