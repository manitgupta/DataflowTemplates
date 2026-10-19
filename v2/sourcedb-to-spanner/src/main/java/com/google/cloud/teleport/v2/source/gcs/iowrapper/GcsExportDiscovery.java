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

import com.google.cloud.teleport.v2.spanner.migrations.gcs.BulkGcsFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers the tables of a bulk migration Avro export (see {@link BulkGcsFormat}) on the launcher.
 *
 * <p>Tables are the first-level directories under the root that contain {@code *.avro} files (at
 * any depth, e.g. {@code root/table/shard/file.avro}). A table's payload schema and primary keys
 * are read from file headers and its first record. Files of one table that were written with
 * different schemas have different name prefixes ({@code <md5Hex(schema)>-NNNNN-of-MMMMM.avro}):
 * one file per prefix is inspected, and the payload schemas must be identical.
 */
public final class GcsExportDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(GcsExportDiscovery.class);

  private GcsExportDiscovery() {}

  /**
   * Lists the Avro files of every table under {@code root}.
   *
   * @param root root directory of the export, without trailing {@code /}.
   * @return table name to its files sorted by path, sorted by table name.
   * @throws IllegalArgumentException if listing fails.
   */
  public static ImmutableSortedMap<String, ImmutableList<ResourceId>> listTableFiles(String root) {
    String base = BulkGcsFormat.stripTrailingSlash(root);
    String pattern = base + "/*/**.avro";
    MatchResult result;
    try {
      result = FileSystems.match(pattern, EmptyMatchTreatment.ALLOW);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to list the Avro export files " + pattern, e);
    }
    List<MatchResult.Metadata> files;
    try {
      files = result.metadata();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to list the Avro export files " + pattern, e);
    }
    Map<String, List<ResourceId>> byTable = new TreeMap<>();
    String prefix = base + "/";
    for (MatchResult.Metadata file : files) {
      ResourceId id = file.resourceId();
      if (id.isDirectory()) {
        continue;
      }
      String path = id.toString();
      if (!path.startsWith(prefix)) {
        LOG.warn("Ignoring {}: it is not under the export root {}", path, base);
        continue;
      }
      String relative = path.substring(prefix.length());
      int slash = relative.indexOf('/');
      if (slash <= 0) {
        continue;
      }
      byTable.computeIfAbsent(relative.substring(0, slash), t -> new ArrayList<>()).add(id);
    }
    ImmutableSortedMap.Builder<String, ImmutableList<ResourceId>> tables =
        ImmutableSortedMap.naturalOrder();
    for (Map.Entry<String, List<ResourceId>> entry : byTable.entrySet()) {
      List<ResourceId> sorted = new ArrayList<>(entry.getValue());
      sorted.sort((a, b) -> a.toString().compareTo(b.toString()));
      tables.put(entry.getKey(), ImmutableList.copyOf(sorted));
    }
    return tables.build();
  }

  /**
   * Describes one table from its files.
   *
   * @param table table name.
   * @param files the table's files, sorted by path; must not be empty.
   * @throws IllegalArgumentException if a file is not in the bulk format, or the table's files have
   *     different payload schemas.
   */
  public static GcsExportTable describeTable(String table, List<ResourceId> files) {
    if (files.isEmpty()) {
      throw new IllegalArgumentException("Table " + table + " has no Avro files.");
    }
    // One file per schema prefix first, then (only if no record was found yet) the others.
    Map<String, ResourceId> firstPerPrefix = new TreeMap<>();
    for (ResourceId file : files) {
      firstPerPrefix.putIfAbsent(schemaPrefix(file.getFilename()), file);
    }
    List<ResourceId> order = new ArrayList<>(firstPerPrefix.values());
    Set<ResourceId> representatives = new HashSet<>(order);
    for (ResourceId file : files) {
      if (!representatives.contains(file)) {
        order.add(file);
      }
    }
    Schema payloadSchema = null;
    ResourceId payloadSchemaFile = null;
    ImmutableList<String> primaryKeys = null;
    for (int i = 0; i < order.size(); i++) {
      boolean prefixRepresentative = i < firstPerPrefix.size();
      if (!prefixRepresentative && primaryKeys != null) {
        break;
      }
      ResourceId file = order.get(i);
      FileHead head = readHead(file);
      Schema filePayload = payloadSchema(head.schema, file);
      if (payloadSchema == null) {
        payloadSchema = filePayload;
        payloadSchemaFile = file;
      } else if (!payloadSchema.equals(filePayload)) {
        throw new IllegalArgumentException(
            String.format(
                "Table %s has files with different payload schemas, which the GCS source does not"
                    + " support: %s has %s but %s has %s. Re-export the table with a single"
                    + " schema.",
                table, payloadSchemaFile, payloadSchema, file, filePayload));
      }
      if (primaryKeys == null && head.firstRecord != null) {
        primaryKeys = primaryKeys(head.firstRecord);
        Object recordTable = head.firstRecord.get(BulkGcsFormat.TABLE_NAME_FIELD);
        if (recordTable != null && !table.equals(recordTable.toString())) {
          LOG.warn(
              "Records of directory {} have tableName {}; using the directory name {}",
              table,
              recordTable,
              table);
        }
      }
    }
    if (primaryKeys == null) {
      LOG.warn("Table {} has no records; it has no primary key columns.", table);
      primaryKeys = ImmutableList.of();
    }
    for (String key : primaryKeys) {
      if (payloadSchema.getField(key) == null) {
        LOG.warn("Primary key column {} of table {} is not a payload field.", key, table);
      }
    }
    LOG.info(
        "Discovered table {}: {} files, primary keys {}, payload schema {}",
        table,
        files.size(),
        primaryKeys,
        payloadSchema);
    return GcsExportTable.create(table, payloadSchema, primaryKeys);
  }

  /** Returns the name prefix that identifies the writer schema: the text before the first '-'. */
  static String schemaPrefix(String fileName) {
    int dash = fileName.indexOf('-');
    return dash > 0 ? fileName.substring(0, dash) : fileName;
  }

  private static Schema payloadSchema(Schema schema, ResourceId file) {
    Schema.Field payload =
        schema.getType() == Schema.Type.RECORD
            ? schema.getField(BulkGcsFormat.PAYLOAD_FIELD)
            : null;
    if (payload == null
        || payload.schema().getType() != Schema.Type.RECORD
        || schema.getField(BulkGcsFormat.PRIMARY_KEYS_FIELD) == null
        || schema.getField(BulkGcsFormat.TABLE_NAME_FIELD) == null) {
      throw new IllegalArgumentException(
          String.format(
              "File %s is not in the bulk migration Avro format (a record with fields %s, %s, %s"
                  + " and a record field %s); its schema is %s",
              file,
              BulkGcsFormat.TABLE_NAME_FIELD,
              BulkGcsFormat.SHARD_ID_FIELD,
              BulkGcsFormat.PRIMARY_KEYS_FIELD,
              BulkGcsFormat.PAYLOAD_FIELD,
              schema));
    }
    return payload.schema();
  }

  private static ImmutableList<String> primaryKeys(GenericRecord record) {
    Object keys = record.get(BulkGcsFormat.PRIMARY_KEYS_FIELD);
    if (!(keys instanceof Collection)) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (Object key : (Collection<?>) keys) {
      if (key != null) {
        builder.add(key.toString());
      }
    }
    return builder.build();
  }

  private static FileHead readHead(ResourceId file) {
    try (InputStream in = Channels.newInputStream(FileSystems.open(file));
        DataFileStream<GenericRecord> stream =
            new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {
      Schema schema = stream.getSchema();
      GenericRecord first = stream.hasNext() ? stream.next() : null;
      return new FileHead(schema, first);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to read the Avro file " + file, e);
    }
  }

  private static final class FileHead {
    final Schema schema;
    @Nullable final GenericRecord firstRecord;

    FileHead(Schema schema, @Nullable GenericRecord firstRecord) {
      this.schema = schema;
      this.firstRecord = firstRecord;
    }
  }
}
