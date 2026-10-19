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
package com.google.cloud.teleport.v2.spanner.migrations.gcs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BulkGcsAvroReader}. */
@RunWith(JUnit4.class)
public class BulkGcsAvroReaderTest implements Serializable {

  private static final Schema PAYLOAD =
      SchemaBuilder.record(BulkGcsFormat.PAYLOAD_FIELD).fields().optionalString("id").endRecord();
  private static final Schema RECORD =
      SchemaBuilder.record(BulkGcsFormat.RECORD_NAME)
          .fields()
          .requiredString(BulkGcsFormat.TABLE_NAME_FIELD)
          .optionalString(BulkGcsFormat.SHARD_ID_FIELD)
          .name(BulkGcsFormat.PRIMARY_KEYS_FIELD)
          .type()
          .array()
          .items()
          .stringType()
          .noDefault()
          .name(BulkGcsFormat.PAYLOAD_FIELD)
          .type(PAYLOAD)
          .noDefault()
          .endRecord();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TemporaryFolder tmp = new TemporaryFolder();

  private String root;

  @Before
  public void setUp() throws IOException {
    root = tmp.getRoot().getAbsolutePath();
    write("T1/s1/f.avro", "T1", "s1", "1");
    write("T1/s10/f.avro", "T1", "s10", "2");
    write("T2/f-00000-of-00001.avro", "T2", null, "3");
    write("a*b/f.avro", "a*b", null, "4");
    write("axb/f.avro", "axb", null, "5");
  }

  private void write(String path, String table, String shard, String id) throws IOException {
    File file = new File(tmp.getRoot(), path);
    file.getParentFile().mkdirs();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(RECORD))) {
      writer.create(RECORD, file);
      GenericRecord payload = new GenericData.Record(PAYLOAD);
      payload.put("id", id);
      GenericRecord record = new GenericData.Record(RECORD);
      record.put(BulkGcsFormat.TABLE_NAME_FIELD, table);
      record.put(BulkGcsFormat.SHARD_ID_FIELD, shard);
      record.put(BulkGcsFormat.PRIMARY_KEYS_FIELD, List.of("id"));
      record.put(BulkGcsFormat.PAYLOAD_FIELD, payload);
      writer.append(record);
    }
  }

  private PCollection<String> ids(BulkGcsAvroReader reader) {
    return pipeline
        .apply(reader)
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    r ->
                        ((GenericRecord) r.get(BulkGcsFormat.PAYLOAD_FIELD)).get("id").toString()));
  }

  @Test
  public void readsAllFilesWithoutFilters() {
    PAssert.that(ids(BulkGcsAvroReader.of(root + "/", null, null)))
        .containsInAnyOrder("1", "2", "3", "4", "5");
    pipeline.run();
  }

  @Test
  public void readsSelectedTables() {
    PAssert.that(ids(BulkGcsAvroReader.of(root, Set.of("T1", "T2"), null)))
        .containsInAnyOrder("1", "2", "3");
    pipeline.run();
  }

  @Test
  public void readsSelectedTablesAndShards() {
    PAssert.that(ids(BulkGcsAvroReader.of(root, Set.of("T1"), Set.of("s1"))))
        .containsInAnyOrder("1");
    pipeline.run();
  }

  @Test
  public void readsSelectedShardsOnly() {
    PAssert.that(ids(BulkGcsAvroReader.of(root, null, Set.of("s10")))).containsInAnyOrder("2");
    pipeline.run();
  }

  @Test
  public void readsTableWithGlobCharacterOnly() {
    PAssert.that(ids(BulkGcsAvroReader.of(root, Set.of("a*b"), null))).containsInAnyOrder("4");
    pipeline.run();
  }

  @Test
  public void missingTableReadsNothing() {
    PAssert.that(ids(BulkGcsAvroReader.of(root, Set.of("nope"), null))).empty();
    pipeline.run();
  }

  @Test
  public void waitsOnSignal() {
    PCollection<String> signal =
        pipeline.apply("Signal", Create.of("x").withCoder(StringUtf8Coder.of()));
    PAssert.that(
            ids(BulkGcsAvroReader.of(root, Set.of("T2"), null).withWaitOnSignal(Wait.on(signal))))
        .containsInAnyOrder("3");
    pipeline.run();
  }

  @Test
  public void filePatterns() {
    assertEquals(
        List.of(root + "/T2/**.avro"),
        BulkGcsAvroReader.of(root + "/", List.of("T2"), List.of()).getFilePatterns());
  }

  @Test
  public void blankRootThrows() {
    assertThrows(IllegalArgumentException.class, () -> BulkGcsAvroReader.of(" ", null, null));
    assertThrows(IllegalArgumentException.class, () -> BulkGcsAvroReader.of(null, null, null));
  }

  @Test
  public void identityFnReturnsInput() {
    GenericRecord record = new GenericData.Record(PAYLOAD);
    assertEquals(record, new IdentityGenericRecordFn().apply(record));
  }
}
