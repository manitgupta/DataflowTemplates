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
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.iowrapper.GcsIoWrapper;
import com.google.common.io.Resources;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checks that the checked-in fixtures of {@code GcsSourceDbToSpannerIT} are a valid bulk-format
 * export as written by the ArangoDB exporter: {@code
 * <tableName>/<md5Hex(schema)>-00000-of-00001.avro} with the exporter's golden schema, and the GCS
 * source discovers them.
 */
@RunWith(JUnit4.class)
public class GcsItFixturesTest {

  private static final String EXPORT = "GcsSourceDbToSpannerIT/export";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String golden(String file) throws Exception {
    return new String(
        Files.readAllBytes(
            Paths.get("tools/arangodb-gcs-exporter/src/main/resources/avro").resolve(file)),
        StandardCharsets.UTF_8);
  }

  @Test
  public void itFixturesAreBulkFormatExport() throws Exception {
    File root = new File(Resources.getResource(EXPORT).toURI());
    Map<String, String> schemas =
        Map.of(
            "user_profiles", golden("arangodb-document.avsc"),
            "knows", golden("arangodb-edge.avsc"));
    Map<String, Long> rows = Map.of("user_profiles", 3L, "knows", 2L);
    for (Map.Entry<String, String> table : schemas.entrySet()) {
      File[] files = new File(root, table.getKey()).listFiles();
      assertThat(files).hasLength(1);
      assertThat(files[0].getName())
          .isEqualTo(DigestUtils.md5Hex(table.getValue()) + "-00000-of-00001.avro");
      long count = 0;
      try (DataFileReader<GenericRecord> reader =
          new DataFileReader<>(files[0], new GenericDatumReader<>())) {
        assertThat(reader.getSchema().toString()).isEqualTo(table.getValue());
        while (reader.hasNext()) {
          GenericRecord record = reader.next();
          assertThat(record.get("tableName").toString()).isEqualTo(table.getKey());
          assertThat(record.get("shardId")).isNull();
          assertThat(record.get("primaryKeys").toString()).isEqualTo("[key]");
          GenericRecord payload = (GenericRecord) record.get("payload");
          assertThat(MAPPER.readTree(payload.get("doc").toString()).get("_key").asText())
              .isEqualTo(payload.get("key").toString());
          count++;
        }
      }
      assertThat(count).isEqualTo(rows.get(table.getKey()));
    }

    List<SourceTableSchema> discovered =
        new GcsIoWrapper(root.getAbsolutePath(), List.of(), null)
            .discoverTableSchema()
            .get(0)
            .tableSchemas();
    assertThat(discovered.stream().map(SourceTableSchema::tableName).collect(Collectors.toList()))
        .containsExactly("knows", "user_profiles");
  }
}
