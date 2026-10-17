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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsSourceConfig}. */
@RunWith(JUnit4.class)
public class GcsSourceConfigTest {

  @Test
  public void testValid() {
    GcsSourceConfig config = GcsSourceConfig.fromUri("gs://b/exp/manifest.json");
    assertThat(config.getManifestPath()).isEqualTo("gs://b/exp/manifest.json");
    assertThat(new GcsSourceConfig("/tmp/m.json").getManifestPath()).isEqualTo("/tmp/m.json");
  }

  @Test
  public void testTrailingSlashAppendsManifest() {
    assertThat(GcsSourceConfig.fromUri("gs://b/exp/").getManifestPath())
        .isEqualTo("gs://b/exp/" + GcsSourceConfig.MANIFEST_FILE_NAME);
    assertThat(GcsSourceConfig.fromUri(" gs://b/exp/ ").getManifestPath())
        .isEqualTo("gs://b/exp/manifest.json");
  }

  @Test
  public void testBlankOrNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> GcsSourceConfig.fromUri(null));
    assertThrows(IllegalArgumentException.class, () -> GcsSourceConfig.fromUri(""));
    assertThrows(IllegalArgumentException.class, () -> GcsSourceConfig.fromUri("   "));
    assertThrows(IllegalArgumentException.class, () -> new GcsSourceConfig(null));
    assertThrows(IllegalArgumentException.class, () -> new GcsSourceConfig(" "));
  }

  @Test
  public void testEqualsHashCodeToString() {
    GcsSourceConfig a = GcsSourceConfig.fromUri("gs://b/exp/");
    GcsSourceConfig b = new GcsSourceConfig("gs://b/exp/manifest.json");
    GcsSourceConfig c = new GcsSourceConfig("gs://b/other/manifest.json");
    assertThat(a).isEqualTo(b);
    assertThat(a).isEqualTo(a);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(c);
    assertThat(a).isNotEqualTo("gs://b/exp/manifest.json");
    assertThat(a.toString()).contains("gs://b/exp/manifest.json");
    assertThat(SerializableUtils.clone(a)).isEqualTo(a);
  }
}
