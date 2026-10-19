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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The export summary ({@code arangodb-export.json}, {@code formatVersion} 2): source information,
 * consistency, per-table counts, revisions, data files and statistics. It is informational (for
 * operators and the e2e validation); the SourceDB to Spanner template does not read it, it reads
 * the bulk GCS layout ({@code <table>/<md5>-NNNNN-of-MMMMM.avro}) directly. Null fields are
 * omitted.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"formatVersion", "fileFormat", "exportId", "producer", "source", "tables"})
public final class ExportSummary implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int FORMAT_VERSION = 2;
  public static final String FILE_FORMAT_AVRO = "AVRO";

  public int formatVersion = FORMAT_VERSION;
  public String fileFormat = FILE_FORMAT_AVRO;
  public String exportId;
  public String producer;
  public Source source;
  public List<Table> tables = new ArrayList<>();

  /** Information about the exported system. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({
    "system",
    "database",
    "serverVersion",
    "exportStartedAt",
    "exportFinishedAt",
    "consistent"
  })
  public static final class Source implements Serializable {
    private static final long serialVersionUID = 1L;
    public String system;
    public String database;
    public String serverVersion;
    public String exportStartedAt;
    public String exportFinishedAt;
    public Boolean consistent;
  }

  /** One exported collection. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({
    "name",
    "sourceName",
    "collectionType",
    "primaryKey",
    "columns",
    "rowCount",
    "sourceCountBefore",
    "sourceCountAfter",
    "sourceRevisionBefore",
    "sourceRevisionAfter",
    "files",
    "stats"
  })
  public static final class Table implements Serializable {
    private static final long serialVersionUID = 1L;
    public String name;
    public String sourceName;
    public String collectionType;
    public List<String> primaryKey = new ArrayList<>();
    public List<Column> columns = new ArrayList<>();
    public Long rowCount;
    public Long sourceCountBefore;
    public Long sourceCountAfter;
    public String sourceRevisionBefore;
    public String sourceRevisionAfter;
    public List<FileEntry> files;
    public Stats stats;
  }

  /** One column of the Avro {@code payload} record. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"name", "type", "nullable", "sourceName"})
  public static final class Column implements Serializable {
    private static final long serialVersionUID = 1L;
    public String name;
    public String type;
    public boolean nullable = true;
    public String sourceName;

    public Column() {}

    public Column(String name, String type, boolean nullable, String sourceName) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.sourceName = sourceName;
    }
  }

  /** One data file, relative to the export root, with its Avro record count. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"path", "rows"})
  public static final class FileEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    public String path;
    public long rows;

    public FileEntry() {}

    public FileEntry(String path, long rows) {
      this.path = path;
      this.rows = rows;
    }
  }

  /** Informational statistics. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"maxDocBytes", "oversizedKeys"})
  public static final class Stats implements Serializable {
    private static final long serialVersionUID = 1L;
    public long maxDocBytes;
    public List<String> oversizedKeys = new ArrayList<>();

    public Stats() {}

    public Stats(long maxDocBytes, List<String> oversizedKeys) {
      this.maxDocBytes = maxDocBytes;
      this.oversizedKeys = oversizedKeys;
    }
  }
}
