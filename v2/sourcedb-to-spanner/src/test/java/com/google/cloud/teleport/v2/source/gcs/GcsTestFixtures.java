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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/** Test fixtures for the GCS source: Avro schemas of the ArangoDB exporter and export writers. */
public final class GcsTestFixtures {

  public static final Schema DOCUMENT_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\": \"record\", \"name\": \"ArangoDocumentRow\", \"namespace\":"
                  + " \"com.google.cloud.teleport.v2.tools.arangodb\", \"fields\": ["
                  + "{\"name\": \"key\", \"type\": \"string\"},"
                  + "{\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},"
                  + "{\"name\": \"rev\", \"type\": [\"null\", \"string\"], \"default\": null},"
                  + "{\"name\": \"doc\", \"type\": \"string\"}]}");

  public static final Schema EDGE_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\": \"record\", \"name\": \"ArangoEdgeRow\", \"namespace\":"
                  + " \"com.google.cloud.teleport.v2.tools.arangodb\", \"fields\": ["
                  + "{\"name\": \"key\", \"type\": \"string\"},"
                  + "{\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},"
                  + "{\"name\": \"rev\", \"type\": [\"null\", \"string\"], \"default\": null},"
                  + "{\"name\": \"from_id\", \"type\": \"string\"},"
                  + "{\"name\": \"to_id\", \"type\": \"string\"},"
                  + "{\"name\": \"doc\", \"type\": \"string\"}]}");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GcsTestFixtures() {}

  public static GenericRecord document(String key, String doc) {
    GenericRecord r = new GenericData.Record(DOCUMENT_SCHEMA);
    r.put("key", key);
    r.put("id", "user_profiles/" + key);
    r.put("rev", "_rev" + key);
    r.put("doc", doc);
    return r;
  }

  public static GenericRecord edge(String key, String from, String to, String doc) {
    GenericRecord r = new GenericData.Record(EDGE_SCHEMA);
    r.put("key", key);
    r.put("id", "knows/" + key);
    r.put("rev", "_rev" + key);
    r.put("from_id", from);
    r.put("to_id", to);
    r.put("doc", doc);
    return r;
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

  /** Writes the records to a temp Avro file and reads them back (values become Avro types). */
  public static List<GenericRecord> roundTrip(File dir, Schema schema, List<GenericRecord> records)
      throws IOException {
    File f = writeAvro(new File(dir, "roundtrip-" + System.nanoTime() + ".avro"), schema, records);
    List<GenericRecord> out = new ArrayList<>();
    try (DataFileReader<GenericRecord> reader =
        new DataFileReader<>(f, new GenericDatumReader<GenericRecord>())) {
      while (reader.hasNext()) {
        out.add(reader.next());
      }
    }
    return out;
  }

  public static ObjectNode column(String name, String type, boolean nullable, String sourceName) {
    ObjectNode c = MAPPER.createObjectNode();
    c.put("name", name);
    c.put("type", type);
    c.put("nullable", nullable);
    c.put("sourceName", sourceName);
    return c;
  }

  /**
   * Writes an export with tables {@code user_profiles} (documents u1, u2 in two files and u3 in a
   * third) and {@code knows} (edges e1, e2) under {@code dir} and returns the manifest path.
   *
   * @param consistent value of {@code source.consistent}.
   * @param userProfilesRowCount manifest rowCount of user_profiles, or null to omit it.
   */
  public static String writeExport(File dir, boolean consistent, Long userProfilesRowCount)
      throws IOException {
    writeAvro(
        new File(dir, "tables/user_profiles/part-00000.avro"),
        DOCUMENT_SCHEMA,
        List.of(
            document("u1", "{\"_key\":\"u1\",\"name\":\"Ann\",\"tags\":[1,\"x\",null]}"),
            document("u2", "{\"_key\":\"u2\",\"nested\":{\"a\":{}}}")));
    writeAvro(
        new File(dir, "tables/user_profiles/part-00001.avro"),
        DOCUMENT_SCHEMA,
        List.of(document("u3", "{\"_key\":\"u3\",\"emoji\":\"\ud83d\ude00\"}")));
    writeAvro(
        new File(dir, "tables/knows/part-00000.avro"),
        EDGE_SCHEMA,
        List.of(
            edge("e1", "user_profiles/u1", "user_profiles/u2", "{\"_key\":\"e1\"}"),
            edge("e2", "user_profiles/u2", "missing/zz", "{\"_key\":\"e2\"}")));

    ObjectNode root = MAPPER.createObjectNode();
    root.put("formatVersion", 1);
    root.put("fileFormat", "AVRO");
    root.put("exportId", "20261017T100000Z");
    root.put("someFutureField", "ignored");
    ObjectNode source = root.putObject("source");
    source.put("system", "arangodb");
    source.put("exportStartedAt", "2026-10-17T10:00:00Z");
    source.put("consistent", consistent);
    ArrayNode tables = root.putArray("tables");

    ObjectNode users = tables.addObject();
    users.put("name", "user_profiles");
    users.put("sourceName", "user-profiles");
    users.put("collectionType", "DOCUMENT");
    users.putArray("primaryKey").add("key");
    ArrayNode userCols = users.putArray("columns");
    userCols.add(column("key", "STRING", false, "_key"));
    userCols.add(column("id", "STRING", true, "_id"));
    userCols.add(column("rev", "STRING", true, "_rev"));
    userCols.add(column("doc", "JSON", false, "$"));
    if (userProfilesRowCount != null) {
      users.put("rowCount", userProfilesRowCount);
    }
    ArrayNode userFiles = users.putArray("files");
    userFiles.addObject().put("path", "tables/user_profiles/part-00000.avro").put("rows", 2);
    userFiles.addObject().put("path", "tables/user_profiles/part-00001.avro").put("rows", 1);

    ObjectNode knows = tables.addObject();
    knows.put("name", "knows");
    knows.put("collectionType", "EDGE");
    knows.putArray("primaryKey").add("key");
    ArrayNode knowsCols = knows.putArray("columns");
    knowsCols.add(column("key", "STRING", false, "_key"));
    knowsCols.add(column("id", "STRING", true, "_id"));
    knowsCols.add(column("rev", "STRING", true, "_rev"));
    knowsCols.add(column("from_id", "STRING", false, "_from"));
    knowsCols.add(column("to_id", "STRING", false, "_to"));
    knowsCols.add(column("doc", "JSON", false, "$"));
    knows.put("rowCount", 2);
    knows.put("filePattern", "tables/knows/*.avro");

    File manifest = new File(dir, "manifest.json");
    Files.write(manifest.toPath(), MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(root));
    return manifest.getAbsolutePath();
  }

  /** Reads a test resource as a string. */
  public static String resource(String name) throws IOException {
    try (var in = GcsTestFixtures.class.getClassLoader().getResourceAsStream(name)) {
      if (in == null) {
        throw new IOException("Missing resource " + name);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
