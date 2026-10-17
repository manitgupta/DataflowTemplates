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
package com.google.cloud.teleport.v2.source.gcs;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsExportManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsFileManifest;
import com.google.cloud.teleport.v2.source.gcs.manifest.GcsTableManifest;
import com.google.common.io.Resources;
import java.io.File;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checks that the checked-in fixtures of {@code GcsSourceDbToSpannerIT} are a valid export: the
 * manifest parses, every listed file exists with the declared row count, and {@code doc} is JSON.
 */
@RunWith(JUnit4.class)
public class GcsItFixturesTest {

  private static final String MANIFEST = "GcsSourceDbToSpannerIT/export/manifest.json";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUpFileSystems() {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
  }

  @Test
  public void itFixturesAreConsistentWithManifest() throws Exception {
    File manifestFile = new File(Resources.getResource(MANIFEST).toURI());
    GcsExportManifest manifest = GcsExportManifest.read(manifestFile.getAbsolutePath());

    assertThat(manifest.getTables()).hasSize(2);
    for (GcsTableManifest table : manifest.getTables()) {
      long tableRows = 0;
      for (GcsFileManifest file : table.getFiles()) {
        long fileRows = 0;
        try (DataFileReader<GenericRecord> reader =
            new DataFileReader<>(
                new File(manifestFile.getParentFile(), file.getPath()),
                new GenericDatumReader<>())) {
          while (reader.hasNext()) {
            GenericRecord record = reader.next();
            for (var column : table.getColumns()) {
              assertThat(record.hasField(column.getName())).isTrue();
            }
            assertThat(MAPPER.readTree(record.get("doc").toString()).get("_key").asText())
                .isEqualTo(record.get("key").toString());
            fileRows++;
          }
        }
        assertThat(fileRows).isEqualTo(file.getRows());
        tableRows += fileRows;
      }
      assertThat(tableRows).isEqualTo(table.getRowCount());
    }
  }
}
