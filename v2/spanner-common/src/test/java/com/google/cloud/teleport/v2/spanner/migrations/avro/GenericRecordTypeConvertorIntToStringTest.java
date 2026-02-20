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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericRecordTypeConvertorIntToStringTest {

  @Test
  public void testIntToStringConversion() {
    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    GenericRecordTypeConvertor convertor =
        new GenericRecordTypeConvertor(mockSchemaMapper, "namespace", "shardId", null);

    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("id")
            .type()
            .intType()
            .noDefault()
            .endRecord();

    Object recordValue = 123;

    Value result =
        convertor.getSpannerValue(
            recordValue,
            schema.getField("id").schema(),
            "id",
            Type.string(),
            null // CassandraAnnotations
            );

    assertThat(result).isEqualTo(Value.string("123"));
  }
}
