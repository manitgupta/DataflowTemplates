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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRecordTypeConvertorReproductionTest {

  @Test
  public void testOrderIDIntegerMapping() throws Exception {
    // Setup
    String namespace = "test-namespace";
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    GenericRecordTypeConvertor convertor =
        new GenericRecordTypeConvertor(schemaMapper, namespace, "shard1", null);

    // Mock SchemaMapper behavior
    when(schemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(schemaMapper.getSpannerTableName(namespace, "Orders")).thenReturn("Orders");
    when(schemaMapper.getSpannerColumns(namespace, "Orders"))
        .thenReturn(java.util.Collections.singletonList("OrderID"));
    when(schemaMapper.colExistsAtSource(namespace, "Orders", "OrderID")).thenReturn(true);
    when(schemaMapper.getSourceColumnName(namespace, "Orders", "OrderID")).thenReturn("OrderID");
    when(schemaMapper.getSpannerColumnType(namespace, "Orders", "OrderID"))
        .thenReturn(Type.int64());

    // Create Input Record
    Schema schema =
        SchemaBuilder.record("Orders")
            .fields()
            .name("OrderID")
            .type()
            .intType()
            .noDefault()
            .endRecord();
    GenericRecord record =
        new GenericRecordBuilder(schema)
            .set("OrderID", 12345) // Integer value
            .build();

    // Execute
    Map<String, Value> result = convertor.transformChangeEvent(record, "Orders");

    // Verify
    assertEquals(Value.int64(12345L), result.get("OrderID"));
  }
}
