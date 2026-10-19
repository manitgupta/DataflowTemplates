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

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.gcs.mappings.GcsMappingsProvider;
import com.google.cloud.teleport.v2.source.gcs.schema.GcsSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Test fixtures for the GCS source: ArangoDB-style document and edge tables built with the
 * template's own {@link SourceTableSchema} (STRING and JSON columns), and writers of the bulk
 * migration Avro format ({@code root/<tableName>/[<shard>/]<md5Hex(schema)>-NNNNN-of-MMMMM.avro}).
 */
public final class GcsTestFixtures {

  public static final SourceSchemaReference REF =
      SourceSchemaReference.ofGcs(GcsSchemaReference.builder().setExportId("fixture").build());

  public static final long READ_TIME_MICROS = 1_760_000_000_000_000L;

  private GcsTestFixtures() {}

  private static SourceColumnType type(String name) {
    return new SourceColumnType(name, new Long[] {}, new Long[] {});
  }

  /** Document collection: key, id, rev STRING; doc JSON; primary key key. */
  public static SourceTableSchema documentTable(String table) {
    return SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
        .setTableName(table)
        .setPrimaryKeyColumns(ImmutableList.of("key"))
        .addSourceColumnNameToSourceColumnType("key", type("STRING"))
        .addSourceColumnNameToSourceColumnType("id", type("STRING"))
        .addSourceColumnNameToSourceColumnType("rev", type("STRING"))
        .addSourceColumnNameToSourceColumnType("doc", type("JSON"))
        .build();
  }

  /** Edge collection: key, id, rev, from_id, to_id STRING; doc JSON; primary key key. */
  public static SourceTableSchema edgeTable(String table) {
    return SourceTableSchema.builder(GcsMappingsProvider.typeMapper())
        .setTableName(table)
        .setPrimaryKeyColumns(ImmutableList.of("key"))
        .addSourceColumnNameToSourceColumnType("key", type("STRING"))
        .addSourceColumnNameToSourceColumnType("id", type("STRING"))
        .addSourceColumnNameToSourceColumnType("rev", type("STRING"))
        .addSourceColumnNameToSourceColumnType("from_id", type("STRING"))
        .addSourceColumnNameToSourceColumnType("to_id", type("STRING"))
        .addSourceColumnNameToSourceColumnType("doc", type("JSON"))
        .build();
  }

  public static SourceRow document(SourceTableSchema schema, String key, String doc) {
    return SourceRow.builder(REF, schema, null, READ_TIME_MICROS)
        .setField("key", key)
        .setField("id", schema.tableName() + "/" + key)
        .setField("rev", "_rev" + key)
        .setField("doc", doc)
        .build();
  }

  public static SourceRow edge(
      SourceTableSchema schema, String key, String from, String to, String doc) {
    return SourceRow.builder(REF, schema, null, READ_TIME_MICROS)
        .setField("key", key)
        .setField("id", schema.tableName() + "/" + key)
        .setField("rev", "_rev" + key)
        .setField("from_id", from)
        .setField("to_id", to)
        .setField("doc", doc)
        .build();
  }

  /**
   * Writes the rows' {@link SourceRow#toGcsRecord()} into one bulk-format file {@code
   * root/<tableName>/[<shard>/]<md5Hex(schema)>-<index>-of-<count>.avro}. Like the template's sink,
   * a non-null {@code shard} is also the records' {@code shardId}.
   */
  public static File writeBulkFile(
      File root, @Nullable String shard, int index, int count, List<SourceRow> rows)
      throws IOException {
    Schema schema = rows.get(0).gcsSchema();
    String json = schema.toString();
    String dir = rows.get(0).tableName() + (shard == null ? "" : "/" + shard);
    String name = String.format("%s-%05d-of-%05d.avro", DigestUtils.md5Hex(json), index, count);
    List<GenericRecord> records = new ArrayList<>();
    for (SourceRow row : rows) {
      GenericRecord record = row.toGcsRecord();
      if (shard != null) {
        record.put("shardId", shard);
      }
      records.add(record);
    }
    return writeAvro(new File(root, dir + "/" + name), schema, records);
  }

  /** Writes the records into a snappy-compressed Avro container file. */
  public static File writeAvro(File file, Schema schema, List<GenericRecord> records)
      throws IOException {
    file.getParentFile().mkdirs();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema))) {
      writer.setCodec(CodecFactory.snappyCodec());
      writer.create(schema, file);
      for (GenericRecord r : records) {
        writer.append(r);
      }
    }
    return file;
  }

  /**
   * Writes an export with tables {@code user_profiles} (documents u1, u2 in one file and u3 in a
   * second) and {@code knows} (edges e1, e2) under {@code root} and returns the root path.
   */
  public static String writeExport(File root) throws IOException {
    SourceTableSchema users = documentTable("user_profiles");
    SourceTableSchema knows = edgeTable("knows");
    writeBulkFile(
        root,
        null,
        0,
        2,
        List.of(
            document(users, "u1", "{\"_key\":\"u1\",\"name\":\"Ann\",\"tags\":[1,\"x\",null]}"),
            document(users, "u2", "{\"_key\":\"u2\",\"nested\":{\"a\":{}}}")));
    writeBulkFile(
        root,
        null,
        1,
        2,
        List.of(document(users, "u3", "{\"_key\":\"u3\",\"emoji\":\"\ud83d\ude00\"}")));
    writeBulkFile(
        root,
        null,
        0,
        1,
        List.of(
            edge(knows, "e1", "user_profiles/u1", "user_profiles/u2", "{\"_key\":\"e1\"}"),
            edge(knows, "e2", "user_profiles/u2", "missing/zz", "{\"_key\":\"e2\"}")));
    return root.getAbsolutePath();
  }
}
