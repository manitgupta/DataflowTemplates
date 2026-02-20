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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraAnnotations;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRecordTypeConvertorSqlServerTest {

  @Test
  public void testIntToLongConversion() {
    // Setup

    String colName = "OrderID";
    Schema avroSchema =
        SchemaBuilder.record("payload")
            .fields()
            .name(colName)
            .type(SchemaBuilder.builder().unionOf().nullType().and().intType().endUnion())
            .noDefault()
            .endRecord();

    GenericRecord record = new GenericData.Record(avroSchema);
    record.put(colName, 12345); // Integer value

    // Mock SchemaMapper
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    when(schemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    // Convertor
    GenericRecordTypeConvertor convertor =
        new GenericRecordTypeConvertor(schemaMapper, "", null, null);

    // Execute
    Value result =
        convertor.getSpannerValue(
            record.get(colName),
            avroSchema.getField(colName).schema(),
            colName,
            Type.int64(),
            mock(CassandraAnnotations.class));

    // Verify
    assertThat(result).isEqualTo(Value.int64(12345L));
  }
}
