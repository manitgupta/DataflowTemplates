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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * How one collection is exported: target table name, envelope schema (Spark and Avro), manifest
 * columns and the requested number of partitions.
 */
public final class CollectionPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String DOCUMENT_AVRO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"ArangoDocumentRow\","
          + "\"namespace\":\"com.google.cloud.teleport.v2.tools.arangodb\",\"fields\":["
          + "{\"name\":\"key\",\"type\":\"string\"},"
          + "{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"rev\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"doc\",\"type\":\"string\"}]}";

  private static final String EDGE_AVRO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"ArangoEdgeRow\","
          + "\"namespace\":\"com.google.cloud.teleport.v2.tools.arangodb\",\"fields\":["
          + "{\"name\":\"key\",\"type\":\"string\"},"
          + "{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"rev\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"from_id\",\"type\":\"string\"},"
          + "{\"name\":\"to_id\",\"type\":\"string\"},"
          + "{\"name\":\"doc\",\"type\":\"string\"}]}";

  private static final List<ExportManifest.Column> DOCUMENT_COLUMNS =
      Collections.unmodifiableList(
          Arrays.asList(
              new ExportManifest.Column("key", "STRING", false, "_key"),
              new ExportManifest.Column("id", "STRING", true, "_id"),
              new ExportManifest.Column("rev", "STRING", true, "_rev"),
              new ExportManifest.Column("doc", "JSON", false, "$")));

  private static final List<ExportManifest.Column> EDGE_COLUMNS =
      Collections.unmodifiableList(
          Arrays.asList(
              new ExportManifest.Column("key", "STRING", false, "_key"),
              new ExportManifest.Column("id", "STRING", true, "_id"),
              new ExportManifest.Column("rev", "STRING", true, "_rev"),
              new ExportManifest.Column("from_id", "STRING", false, "_from"),
              new ExportManifest.Column("to_id", "STRING", false, "_to"),
              new ExportManifest.Column("doc", "JSON", false, "$")));

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

  public String avroSchemaJson() {
    return info.kind() == CollectionKind.EDGE ? edgeAvroSchema() : documentAvroSchema();
  }

  /** Manifest columns, in Avro field order. */
  public List<ExportManifest.Column> columns() {
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

  /** Avro writer schema {@code ArangoDocumentRow}. */
  public static String documentAvroSchema() {
    return DOCUMENT_AVRO_SCHEMA;
  }

  /** Avro writer schema {@code ArangoEdgeRow}. */
  public static String edgeAvroSchema() {
    return EDGE_AVRO_SCHEMA;
  }

  @Override
  public String toString() {
    return "CollectionPlan{" + info + " -> " + tableName + ", partitions=" + partitions + '}';
  }
}
