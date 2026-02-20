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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SqlServerMappingProvider}. */
@RunWith(JUnit4.class)
public class SqlServerMappingProviderTest {

  @Test
  public void testDateTimeMapping() {
    assertThat(SqlServerMappingProvider.getMapping().get("DATETIME"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.TIMESTAMP));
    assertThat(SqlServerMappingProvider.getMapping().get("DATETIME2"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.TIMESTAMP));
    assertThat(SqlServerMappingProvider.getMapping().get("SMALLDATETIME"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.TIMESTAMP));
    assertThat(SqlServerMappingProvider.getMapping().get("DATETIMEOFFSET"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.TIMESTAMP));
  }

  @Test
  public void testOtherMappings() {
    assertThat(SqlServerMappingProvider.getMapping().get("INT"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.INTEGER));
    assertThat(SqlServerMappingProvider.getMapping().get("VARCHAR"))
        .isEqualTo(UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.STRING));
  }
}
