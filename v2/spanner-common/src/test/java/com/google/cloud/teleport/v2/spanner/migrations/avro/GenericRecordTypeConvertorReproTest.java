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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRecordTypeConvertorReproTest {

  @Test
  public void testOrderIDMappingIssue() throws InvalidTransformationException {
    final String tableName = "Orders";
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable(tableName)
            .column("OrderID")
            .int64()
            .notNull()
            .endColumn()
            .endTable()
            .build();

    final ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        new GenericRecordTypeConvertor(schemaMapper, "", null, null);

    Schema payloadSchema =
        SchemaBuilder.record("payload")
            .fields()
            .name("OrderID")
            .type(SchemaBuilder.builder().intType())
            .noDefault()
            .endRecord();

    GenericRecord payload = new GenericRecordBuilder(payloadSchema).set("OrderID", 3).build();

    Map<String, Value> m = genericRecordTypeConvertor.transformChangeEvent(payload, tableName);
    assertThat(m.get("OrderID")).isEqualTo(Value.int64(3L));
  }

  @Test
  public void testOrderIDMissingInSource() throws InvalidTransformationException {
    final String tableName = "Orders";
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable(tableName)
            .column("OrderID")
            .int64()
            .endColumn()
            .endTable()
            .build();

    final ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        new GenericRecordTypeConvertor(schemaMapper, "", null, null);

    // Payload schema WITHOUT OrderID
    Schema payloadSchema =
        SchemaBuilder.record("payload")
            .fields()
            .name("OtherColumn")
            .type(SchemaBuilder.builder().stringType())
            .noDefault()
            .endRecord();

    GenericRecord payload =
        new GenericRecordBuilder(payloadSchema).set("OtherColumn", "someValue").build();

    Map<String, Value> m = genericRecordTypeConvertor.transformChangeEvent(payload, tableName);
    assertThat(m).doesNotContainKey("OrderID");
  }

  @Test
  public void testOrderIDCaseMismatch() throws InvalidTransformationException {
    final String tableName = "Orders";
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable(tableName)
            .column("OrderID")
            .int64()
            .endColumn()
            .endTable()
            .build();

    final ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        new GenericRecordTypeConvertor(schemaMapper, "", null, null);

    // Payload schema with LOWERCASE orderid
    Schema payloadSchema =
        SchemaBuilder.record("payload")
            .fields()
            .name("orderid")
            .type(SchemaBuilder.builder().intType())
            .noDefault()
            .endRecord();

    GenericRecord payload = new GenericRecordBuilder(payloadSchema).set("orderid", 3).build();

    Map<String, Value> m = genericRecordTypeConvertor.transformChangeEvent(payload, tableName);
    assertThat(m.get("OrderID")).isEqualTo(Value.int64(3L));
  }
}
