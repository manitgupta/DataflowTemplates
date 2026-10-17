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
package com.google.cloud.teleport.v2.source.gcs.manifest;

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsManifestPaths}. */
@RunWith(JUnit4.class)
public class GcsManifestPathsTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void setUpFileSystems() {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
  }

  @Test
  public void testRelativeUnderLocalDir() throws Exception {
    File dir = tmp.newFolder("exp");
    String manifest = new File(dir, "manifest.json").getAbsolutePath();
    assertThat(GcsManifestPaths.resolve(manifest, "tables/t/f.avro"))
        .isEqualTo(new File(dir, "tables/t/f.avro").getAbsolutePath());
  }

  @Test
  public void testRelativeUnderGcs() {
    assertThat(GcsManifestPaths.resolve("gs://my-bucket/p/x/manifest.json", "tables/t/f.avro"))
        .isEqualTo("gs://my-bucket/p/x/tables/t/f.avro");
    assertThat(GcsManifestPaths.resolve("gs://my-bucket/p/x/manifest.json", "tables/t/*.avro"))
        .isEqualTo("gs://my-bucket/p/x/tables/t/*.avro");
  }

  @Test
  public void testAbsoluteUnchanged() {
    assertThat(GcsManifestPaths.resolve("gs://my-bucket/p/x/manifest.json", "gs://other/y/f.avro"))
        .isEqualTo("gs://other/y/f.avro");
    assertThat(GcsManifestPaths.resolve("gs://my-bucket/p/x/manifest.json", "/abs/f.avro"))
        .isEqualTo("/abs/f.avro");
  }
}
