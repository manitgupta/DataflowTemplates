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
package com.google.cloud.teleport.v2.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ComparisonRecordMapperPgTest {

  @Mock private ISchemaMapper mockSchemaMapper;
  @Mock private ISpannerMigrationTransformer mockTransformer;
  @Mock private Ddl mockDdl;

  private ComparisonRecordMapper mapper;

  @Before
  public void setUp() throws InvalidTransformationException {
    mapper = new ComparisonRecordMapper(mockSchemaMapper, mockTransformer, mockDdl);

    // Mock ISchemaMapper
    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn("Users");
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(Arrays.asList("id", "name"));
    when(mockSchemaMapper.colExistsAtSource(anyString(), anyString(), anyString()))
        .thenReturn(true);
    when(mockSchemaMapper.getSourceColumnName(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("id")))
        .thenReturn("id");
    when(mockSchemaMapper.getSourceColumnName(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("name")))
        .thenReturn("name");

    // Mock Ddl
    Table mockTable = mock(Table.class);
    when(mockDdl.table("Users")).thenReturn(mockTable);
    IndexColumn mockIndexColumn = mock(IndexColumn.class);
    when(mockIndexColumn.name()).thenReturn("id");
    when(mockTable.primaryKeys()).thenReturn(ImmutableList.of(mockIndexColumn));

    // Mock Transformer
    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);
  }

  @Test
  public void testMapFromAvroRecord_PgDialect() throws Exception {
    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.POSTGRESQL);

    // Crucial part: mock Spanner column types for PG Dialect
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("id")))
        .thenReturn(Type.pgInt8());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("name")))
        .thenReturn(Type.pgVarchar());

    Table mockTable = mockDdl.table("Users");
    Column mockCol1 = mock(Column.class);
    when(mockCol1.name()).thenReturn("id");
    when(mockCol1.type()).thenReturn(Type.pgInt8());
    Column mockCol2 = mock(Column.class);
    when(mockCol2.name()).thenReturn("name");
    when(mockCol2.type()).thenReturn(Type.pgVarchar());
    when(mockTable.columns()).thenReturn(ImmutableList.of(mockCol1, mockCol2));

    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        Arrays.asList(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 123L);
    payload.put("name", "Bob");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "Users");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals("Users", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("id", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("123", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  @Test
  public void testMapFromAvroRecord_GoogleSqlDialect() throws Exception {
    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    // Crucial part: mock Spanner column types for GSQL Dialect
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("id")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("name")))
        .thenReturn(Type.string());

    Table mockTable = mockDdl.table("Users");
    Column mockCol1 = mock(Column.class);
    when(mockCol1.name()).thenReturn("id");
    when(mockCol1.type()).thenReturn(Type.int64());
    Column mockCol2 = mock(Column.class);
    when(mockCol2.name()).thenReturn("name");
    when(mockCol2.type()).thenReturn(Type.string());
    when(mockTable.columns()).thenReturn(ImmutableList.of(mockCol1, mockCol2));

    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        Arrays.asList(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 456L);
    payload.put("name", "Alice");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "Users");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals("Users", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("id", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("456", record.getPrimaryKeyColumns().get(0).getColValue());
  }
}
