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
package com.google.cloud.teleport.v2.tools.arangodb;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * How one collection is exported: target table name, the Spark read schema, the Avro writer schema
 * of the SourceDB to Spanner bulk GCS format, summary columns and the requested number of
 * partitions.
 *
 * <p>The writer schemas are the resources {@code avro/arangodb-document.avsc} and {@code
 * avro/arangodb-edge.avsc}: byte for byte what the template's {@code SourceRow.gcsSchema()} returns
 * for such a table ({@code SourceRowWithMetadata{tableName, shardId, primaryKeys, payload}}), so
 * the file names ({@code md5Hex(schema)}) and headers equal what the template's own GCS sink
 * writes.
 */
public final class CollectionPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Classpath resource with the bulk GCS writer schema of document collections. */
  static final String DOCUMENT_SCHEMA_RESOURCE = "/avro/arangodb-document.avsc";

  /** Classpath resource with the bulk GCS writer schema of edge collections. */
  static final String EDGE_SCHEMA_RESOURCE = "/avro/arangodb-edge.avsc";

  /** Value of the {@code primaryKeys} field of every record. */
  static final String PRIMARY_KEY = "key";

  private static final String DOCUMENT_AVRO_SCHEMA = readResource(DOCUMENT_SCHEMA_RESOURCE);
  private static final String EDGE_AVRO_SCHEMA = readResource(EDGE_SCHEMA_RESOURCE);
  private static final String DOCUMENT_AVRO_SCHEMA_MD5 = md5Hex(DOCUMENT_AVRO_SCHEMA);
  private static final String EDGE_AVRO_SCHEMA_MD5 = md5Hex(EDGE_AVRO_SCHEMA);

  private static final List<ExportSummary.Column> DOCUMENT_COLUMNS =
      Collections.unmodifiableList(
          Arrays.asList(
              new ExportSummary.Column("key", "STRING", false, "_key"),
              new ExportSummary.Column("id", "STRING", true, "_id"),
              new ExportSummary.Column("rev", "STRING", true, "_rev"),
              new ExportSummary.Column("doc", "JSON", false, "$")));

  private static final List<ExportSummary.Column> EDGE_COLUMNS =
      Collections.unmodifiableList(
          Arrays.asList(
              new ExportSummary.Column("key", "STRING", false, "_key"),
              new ExportSummary.Column("id", "STRING", true, "_id"),
              new ExportSummary.Column("rev", "STRING", true, "_rev"),
              new ExportSummary.Column("from_id", "STRING", false, "_from"),
              new ExportSummary.Column("to_id", "STRING", false, "_to"),
              new ExportSummary.Column("doc", "JSON", false, "$")));

  private final CollectionInfo info;
  private final String tableName;
  private final int partitions;

  private CollectionPlan(CollectionInfo info, String tableName, int partitions) {
    this.info = info;
    this.tableName = tableName;
    this.partitions = partitions;
  }

  /**
   * Creates the plan; the partition count comes from {@link ExporterOptions#partitionsFor(long)}.
   */
  public static CollectionPlan create(
      CollectionInfo info, String tableName, long count, ExporterOptions opts) {
    return new CollectionPlan(info, tableName, opts.partitionsFor(count));
  }

  /** Creates a plan with an explicit partition count (tests). */
  static CollectionPlan create(CollectionInfo info, String tableName, int partitions) {
    return new CollectionPlan(info, tableName, Math.max(1, partitions));
  }

  public CollectionInfo info() {
    return info;
  }

  public String sourceName() {
    return info.sourceName();
  }

  public String tableName() {
    return tableName;
  }

  public CollectionKind kind() {
    return info.kind();
  }

  /** Requested number of partitions (RANGE may plan fewer). */
  public int partitions() {
    return partitions;
  }

  public StructType structType() {
    return info.kind() == CollectionKind.EDGE ? edgeStruct() : documentStruct();
  }

  /** The Avro writer schema text (exactly the resource bytes). */
  public String avroSchemaJson() {
    return info.kind() == CollectionKind.EDGE ? edgeAvroSchema() : documentAvroSchema();
  }

  /** Lower-case hex MD5 of {@link #avroSchemaJson()} (UTF-8): the data file name prefix. */
  public String avroSchemaMd5() {
    return info.kind() == CollectionKind.EDGE ? EDGE_AVRO_SCHEMA_MD5 : DOCUMENT_AVRO_SCHEMA_MD5;
  }

  /**
   * Turns rows of {@link #structType()} into bulk GCS records: {@code tableName} = the table name,
   * {@code shardId} = null, {@code primaryKeys} = {@code ["key"]} and {@code payload} = the row's
   * columns in schema order.
   */
  public Dataset<Row> toBulkRecords(Dataset<Row> rows) {
    StructField[] fields = structType().fields();
    Column[] payload = new Column[fields.length];
    for (int i = 0; i < fields.length; i++) {
      payload[i] = col(fields[i].name());
    }
    return rows.select(
        lit(tableName).as("tableName"),
        lit(null).cast(DataTypes.StringType).as("shardId"),
        array(lit(PRIMARY_KEY)).as("primaryKeys"),
        struct(payload).as("payload"));
  }

  /** Summary (and DDL) columns, in payload field order. */
  public List<ExportSummary.Column> columns() {
    return info.kind() == CollectionKind.EDGE ? EDGE_COLUMNS : DOCUMENT_COLUMNS;
  }

  /** Spark schema of a document row: {@code key, id, rev, doc}. */
  public static StructType documentStruct() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("key", DataTypes.StringType, false),
          DataTypes.createStructField("id", DataTypes.StringType, true),
          DataTypes.createStructField("rev", DataTypes.StringType, true),
          DataTypes.createStructField("doc", DataTypes.StringType, false)
        });
  }

  /** Spark schema of an edge row: {@code key, id, rev, from_id, to_id, doc}. */
  public static StructType edgeStruct() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("key", DataTypes.StringType, false),
          DataTypes.createStructField("id", DataTypes.StringType, true),
          DataTypes.createStructField("rev", DataTypes.StringType, true),
          DataTypes.createStructField("from_id", DataTypes.StringType, false),
          DataTypes.createStructField("to_id", DataTypes.StringType, false),
          DataTypes.createStructField("doc", DataTypes.StringType, false)
        });
  }

  /** Bulk GCS writer schema of a document collection (payload {@code key, id, rev, doc}). */
  public static String documentAvroSchema() {
    return DOCUMENT_AVRO_SCHEMA;
  }

  /**
   * Bulk GCS writer schema of an edge collection (payload {@code key, id, rev, from_id, to_id,
   * doc}).
   */
  public static String edgeAvroSchema() {
    return EDGE_AVRO_SCHEMA;
  }

  /** Lower-case hex MD5 of the UTF-8 bytes of {@code text}. */
  static String md5Hex(String text) {
    try {
      byte[] digest =
          MessageDigest.getInstance("MD5").digest(text.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 is not available", e);
    }
  }

  private static String readResource(String name) {
    try (InputStream in = CollectionPlan.class.getResourceAsStream(name)) {
      if (in == null) {
        throw new IllegalStateException("Missing classpath resource " + name);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read classpath resource " + name, e);
    }
  }

  @Override
  public String toString() {
    return "CollectionPlan{" + info + " -> " + tableName + ", partitions=" + partitions + '}';
  }
}
