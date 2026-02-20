/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;

/** SQL Server specific JDBC value mappings. */
public class SqlServerJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueMapper<BigDecimal> bigDecimalToByteArray =
      (value, schema) ->
          ByteBuffer.wrap(
              value
                  .setScale((int) schema.getObjectProp("scale"), RoundingMode.HALF_DOWN)
                  .unscaledValue()
                  .toByteArray());

  private static final ResultSetValueExtractor<ByteBuffer> bytesExtractor =
      (rs, fieldName) -> {
        byte[] bytes = rs.getBytes(fieldName);
        return bytes == null ? null : ByteBuffer.wrap(bytes);
      };

  private static final ResultSetValueMapper<Date> dateToAvro =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  private static final ResultSetValueExtractor<java.sql.Date> utcDateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, utcCalendar);
  private static final ResultSetValueExtractor<java.sql.Timestamp> utcTimeStampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName, utcCalendar);

  private static long instantToMicro(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> instantToMicro(value.toInstant());

  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroDateTime =
      (value, schema) ->
          new GenericRecordBuilder(DateTime.SCHEMA)
              .set(
                  DateTime.DATE_FIELD_NAME,
                  (int) value.toLocalDateTime().toLocalDate().toEpochDay())
              .set(
                  DateTime.TIME_FIELD_NAME,
                  TimeUnit.NANOSECONDS.toMicros(
                      value.toLocalDateTime().toLocalTime().toNanoOfDay()))
              .build();

  private static final ResultSetValueMapper<byte[]> binaryToLong =
      (value, schema) -> ByteBuffer.wrap(value).getLong();

  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("BIT", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("CHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(utcDateExtractor, dateToAvro))
          .put("DATETIME", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroDateTime))
          .put("DATETIME2", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroDateTime))
          .put("DATETIMEOFFSET", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put("DECIMAL", Pair.of(ResultSet::getBigDecimal, bigDecimalToByteArray))
          .put("FLOAT", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("IMAGE", Pair.of(bytesExtractor, valuePassThrough))
          .put("INT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("MONEY", Pair.of(ResultSet::getBigDecimal, bigDecimalToByteArray))
          .put("NCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("NTEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("NUMERIC", Pair.of(ResultSet::getBigDecimal, bigDecimalToByteArray))
          .put("NVARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("REAL", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("SMALLDATETIME", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroDateTime))
          .put("SMALLINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SMALLMONEY", Pair.of(ResultSet::getBigDecimal, bigDecimalToByteArray))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIME", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIMESTAMP", Pair.of(ResultSet::getBytes, binaryToLong))
          .put("TINYINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("UNIQUEIDENTIFIER", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARBINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("XML", Pair.of(ResultSet::getString, valuePassThrough))
          .build()
          .entrySet()
          .stream()
          .map(
              entry ->
                  Map.entry(
                      entry.getKey(),
                      new JdbcValueMapper<>(
                          entry.getValue().getLeft(), entry.getValue().getRight())))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return SCHEMA_MAPPINGS;
  }
}
