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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SqlServerJdbcValueMappings}. */
@RunWith(MockitoJUnitRunner.class)
public class SqlServerJdbcValueMappingsTest {

  @Test
  public void testBigIntMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("BIGINT");
    ResultSet rs = mock(ResultSet.class);
    when(rs.getLong(anyString())).thenReturn(123456789L);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertEquals(123456789L, value);
  }

  @Test
  public void testIntMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("INT");
    ResultSet rs = mock(ResultSet.class);
    when(rs.getInt(anyString())).thenReturn(12345);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertEquals(12345, value);
  }

  @Test
  public void testStringMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("VARCHAR");
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(anyString())).thenReturn("test");

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertEquals("test", value);
  }

  @Test
  public void testDecimalMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("DECIMAL");
    ResultSet rs = mock(ResultSet.class);
    BigDecimal bd = new BigDecimal("123.456");
    when(rs.getBigDecimal(anyString())).thenReturn(bd);

    // We need to pass a Schema object that has "scale" prop if the mapper uses it.
    // The mapper code: schema.getObjectProp("scale")
    // But Schema.getField("scale") is for fields, getObjectProp is for json props.
    // Actually Avro Schema has getObjectProp.
    // Let's create a schema with prop.
    Schema decimalSchema = SchemaBuilder.builder().bytesType();
    decimalSchema.addProp("scale", 3);

    Object value = mapper.mapValue(rs, "col", decimalSchema);
    Assert.assertTrue(value instanceof ByteBuffer);
  }

  @Test
  public void testDateMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("DATE");
    ResultSet rs = mock(ResultSet.class);
    java.sql.Date date = java.sql.Date.valueOf("2023-01-01");
    // The mapper uses rs.getDate(fieldName, utcCalendar)
    when(rs.getDate(anyString(), any(Calendar.class))).thenReturn(date);

    Object value = mapper.mapValue(rs, "col", null);
    // 2023-01-01 is 19358 days after epoch
    Assert.assertEquals(19358, value);
  }

  @Test
  public void testDateTimeMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("DATETIME");
    ResultSet rs = mock(ResultSet.class);
    Timestamp ts = Timestamp.valueOf("2023-01-01 12:00:00");
    when(rs.getTimestamp(anyString(), any(Calendar.class))).thenReturn(ts);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertTrue(value instanceof GenericRecord);
    GenericRecord record = (GenericRecord) value;
    Assert.assertNotNull(record.get(DateTime.DATE_FIELD_NAME));
    Assert.assertNotNull(record.get(DateTime.TIME_FIELD_NAME));
  }

  @Test
  public void testDateTime2Mapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("DATETIME2");
    ResultSet rs = mock(ResultSet.class);
    Timestamp ts = Timestamp.valueOf("2023-01-01 12:00:00");
    when(rs.getTimestamp(anyString(), any(Calendar.class))).thenReturn(ts);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertTrue(value instanceof GenericRecord);
  }

  @Test
  public void testSmallDateTimeMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("SMALLDATETIME");
    ResultSet rs = mock(ResultSet.class);
    Timestamp ts = Timestamp.valueOf("2023-01-01 12:00:00");
    when(rs.getTimestamp(anyString(), any(Calendar.class))).thenReturn(ts);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertTrue(value instanceof GenericRecord);
  }

  @Test
  public void testBinaryMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("BINARY");
    ResultSet rs = mock(ResultSet.class);
    byte[] bytes = new byte[] {0x01, 0x02};
    when(rs.getBytes(anyString())).thenReturn(bytes);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertEquals(ByteBuffer.wrap(bytes), value);
  }

  @Test
  public void testRowversionMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("TIMESTAMP");
    ResultSet rs = mock(ResultSet.class);
    byte[] bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 1};
    when(rs.getBytes(anyString())).thenReturn(bytes);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertEquals(1L, value);
  }

  @Test
  public void testDateTimeOffsetMapping() throws SQLException {
    SqlServerJdbcValueMappings mappings = new SqlServerJdbcValueMappings();
    JdbcValueMapper<?> mapper = mappings.getMappings().get("DATETIMEOFFSET");
    ResultSet rs = mock(ResultSet.class);
    Timestamp ts = Timestamp.valueOf("2023-01-01 12:00:00");
    when(rs.getTimestamp(anyString(), any(Calendar.class))).thenReturn(ts);

    Object value = mapper.mapValue(rs, "col", null);
    Assert.assertTrue(value instanceof Long);
  }
}
