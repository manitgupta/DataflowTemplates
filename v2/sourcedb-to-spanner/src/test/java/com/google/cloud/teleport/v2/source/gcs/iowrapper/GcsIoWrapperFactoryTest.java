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
package com.google.cloud.teleport.v2.source.gcs.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.source.gcs.GcsTestFixtures;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.GcsSourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsIoWrapperFactory}. */
@RunWith(JUnit4.class)
public class GcsIoWrapperFactoryTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testFromConfig() {
    GcsIoWrapperFactory factory =
        GcsIoWrapperFactory.fromConfig(GcsSourceConfig.fromUri("gs://my-bucket/exp/"));
    assertThat(factory.manifestPath()).isEqualTo("gs://my-bucket/exp/manifest.json");
    assertThat(factory).isEqualTo(GcsIoWrapperFactory.create("gs://my-bucket/exp/manifest.json"));
  }

  @Test
  public void testFromWrongConfigThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsIoWrapperFactory.fromConfig(mock(SourceConnectionConfig.class)));
    assertThrows(IllegalArgumentException.class, () -> GcsIoWrapperFactory.fromConfig(null));
  }

  @Test
  public void testGetIoWrapperWithoutWait() throws Exception {
    String manifest = GcsTestFixtures.writeExport(tmp.getRoot(), true, null);
    IoWrapper wrapper = GcsIoWrapperFactory.create(manifest).getIOWrapper(List.of(), null);
    assertThat(wrapper).isInstanceOf(GcsIoWrapper.class);
    assertThat(wrapper.getTableReaders()).hasSize(2);
  }
}
