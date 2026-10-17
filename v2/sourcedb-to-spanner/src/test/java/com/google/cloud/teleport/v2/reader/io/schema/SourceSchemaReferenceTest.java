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
package com.google.cloud.teleport.v2.reader.io.schema;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference.Kind;
import com.google.cloud.teleport.v2.source.cassandra.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.gcs.schema.GcsSchemaReference;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceSchemaReferenceTest {
  @Test
  public void testSourceSchemaReferenceBasic() {
    JdbcSchemaReference jdbcSchemaReference =
        JdbcSchemaReference.builder().setDbName("testDB").build();
    CassandraSchemaReference cassandraSchemaReference =
        CassandraSchemaReference.builder().setKeyspaceName("testKeySpace").build();
    assertThat(SourceSchemaReference.ofJdbc(jdbcSchemaReference).getName())
        .isEqualTo(jdbcSchemaReference.getName());
    assertThat(SourceSchemaReference.ofJdbc(jdbcSchemaReference).getKind()).isEqualTo(Kind.JDBC);
    assertThat(SourceSchemaReference.ofJdbc(jdbcSchemaReference).jdbc())
        .isEqualTo(jdbcSchemaReference);
    assertThat(SourceSchemaReference.ofCassandra(cassandraSchemaReference).getKind())
        .isEqualTo(Kind.CASSANDRA);
    assertThat(SourceSchemaReference.ofCassandra(cassandraSchemaReference).getName())
        .isEqualTo(cassandraSchemaReference.getName());
    assertThat(SourceSchemaReference.ofCassandra(cassandraSchemaReference).cassandra())
        .isEqualTo(cassandraSchemaReference);
  }

  @Test
  public void testSourceSchemaReferenceGcs() {
    GcsSchemaReference gcsSchemaReference =
        GcsSchemaReference.builder().setExportId("20261017T100000Z").build();
    SourceSchemaReference ref = SourceSchemaReference.ofGcs(gcsSchemaReference);
    assertThat(ref.getKind()).isEqualTo(Kind.GCS);
    assertThat(ref.gcs()).isEqualTo(gcsSchemaReference);
    assertThat(ref.getName()).isEqualTo(gcsSchemaReference.getName());
    assertThat(ref.getName()).isEqualTo("GcsExport.20261017T100000Z");
    assertThat(ref)
        .isEqualTo(
            SourceSchemaReference.ofGcs(
                GcsSchemaReference.builder().setExportId("20261017T100000Z").build()));
    assertThat(ref)
        .isNotEqualTo(
            SourceSchemaReference.ofGcs(GcsSchemaReference.builder().setExportId("x").build()));
    assertThat(SerializableUtils.clone(ref)).isEqualTo(ref);
  }
}
