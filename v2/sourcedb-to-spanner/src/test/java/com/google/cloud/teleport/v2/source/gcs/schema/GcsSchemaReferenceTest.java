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
package com.google.cloud.teleport.v2.source.gcs.schema;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Strings;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsSchemaReference}. */
@RunWith(JUnit4.class)
public class GcsSchemaReferenceTest {

  @Test
  public void testGetNameSanitises() {
    assertThat(GcsSchemaReference.builder().setExportId("20261017T100000Z").build().getName())
        .isEqualTo("GcsExport.20261017T100000Z");
    assertThat(GcsSchemaReference.builder().setExportId("a-b.c/d e").build().getName())
        .isEqualTo("GcsExport.a_b_c_d_e");
  }

  @Test
  public void testGetNameTruncates() {
    String name =
        GcsSchemaReference.builder().setExportId(Strings.repeat("x", 100)).build().getName();
    assertThat(name).hasLength(64);
    assertThat(name).startsWith("GcsExport.xxx");
  }

  @Test
  public void testEquality() {
    GcsSchemaReference a = GcsSchemaReference.builder().setExportId("e1").build();
    GcsSchemaReference b = GcsSchemaReference.builder().setExportId("e1").build();
    GcsSchemaReference c = GcsSchemaReference.builder().setExportId("e2").build();
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(c);
    assertThat(a.exportId()).isEqualTo("e1");
    assertThat(SerializableUtils.clone(a)).isEqualTo(a);
  }
}
