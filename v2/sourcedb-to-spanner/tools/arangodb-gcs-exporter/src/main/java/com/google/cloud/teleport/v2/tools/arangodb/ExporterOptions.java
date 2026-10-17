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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Command line options of {@link ArangoDbToGcsExporter}. Arguments have the form {@code
 * --key=value}; unknown keys are rejected. {@code --connectorOption.<key>=<value>} entries are
 * passed verbatim to the ArangoDB Spark connector and override the options computed here.
 */
public final class ExporterOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  static final String CONNECTOR_OPTION_PREFIX = "connectorOption.";
  private static final Pattern EXPORT_ID = Pattern.compile("^[A-Za-z0-9_-]+$");
  private static final DateTimeFormatter EXPORT_ID_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  private static final Set<String> PROTOCOLS = Set.of("http", "http2", "vst");
  private static final Set<String> COMPRESSIONS = Set.of("snappy", "deflate", "uncompressed");

  /** Auto partition count upper bound for HASH partitioning (each partition is a full scan). */
  static final int HASH_MAX_AUTO_PARTITIONS = 64;

  private static final Set<String> KNOWN_KEYS =
      new TreeSet<>(
          Arrays.asList(
              "endpoints",
              "database",
              "user",
              "passwordFile",
              "passwordEnv",
              "password",
              "protocol",
              "sslEnabled",
              "sslCertValue",
              "sslTrustStorePath",
              "sslTrustStorePassword",
              "sslVerifyHost",
              "acquireHostList",
              "collections",
              "excludeCollections",
              "output",
              "exportId",
              "partitioning",
              "partitions",
              "targetDocsPerPartition",
              "maxPartitions",
              "rangesPerWrite",
              "parallelWrites",
              "batchSize",
              "cursorTtlSeconds",
              "maxRecordsPerFile",
              "compression",
              "parallelCollections",
              "ddlEdgeHelpers",
              "allowLiveSource",
              "verifyPartitions",
              "verifyRangePlans",
              "planOnly"));

  private String endpoints;
  private String database = "_system";
  private String user = "root";
  private String passwordFile;
  private String passwordEnv = "ARANGO_PASSWORD";
  private String password;
  private String protocol = "http2";
  private boolean sslEnabled = false;
  private String sslCertValue;
  private String sslTrustStorePath;
  private String sslTrustStorePassword;
  private boolean sslVerifyHost = true;
  private boolean acquireHostList = false;
  private List<String> collections = Collections.emptyList();
  private List<String> excludeCollections = Collections.emptyList();
  private String output;
  private String exportId;
  private PartitioningMode partitioning = PartitioningMode.RANGE;
  private int partitions = 0;
  private long targetDocsPerPartition = 2_000_000L;
  private int maxPartitions = 20_000;
  private int rangesPerWrite = 64;
  private int parallelWrites = 8;
  // Each row carries the document plus its JSON_STRINGIFY copy, so batch memory on coordinators and
  // executors grows with document size: keep the default batch small (about 2-4 MB for 1 KB docs).
  private int batchSize = 1_000;
  private int cursorTtlSeconds = 600;
  private long maxRecordsPerFile = 2_000_000L;
  private String compression = "snappy";
  private int parallelCollections = 1;
  private boolean ddlEdgeHelpers = true;
  private boolean allowLiveSource = false;
  private boolean verifyPartitions = false;
  private boolean verifyRangePlans = true;
  private boolean planOnly = false;
  private final Map<String, String> connectorOptions = new LinkedHashMap<>();

  private ExporterOptions() {}

  /**
   * Parses {@code --key=value} arguments.
   *
   * @throws IllegalArgumentException on unknown keys, malformed values or missing required options
   */
  public static ExporterOptions parse(String[] args) {
    ExporterOptions o = new ExporterOptions();
    Set<String> seen = new TreeSet<>();
    for (String arg : args) {
      if (!arg.startsWith("--") || arg.indexOf('=') < 0) {
        throw new IllegalArgumentException("Expected --key=value but got: " + arg);
      }
      int eq = arg.indexOf('=');
      String key = arg.substring(2, eq);
      String value = arg.substring(eq + 1);
      if (key.startsWith(CONNECTOR_OPTION_PREFIX)) {
        String connectorKey = key.substring(CONNECTOR_OPTION_PREFIX.length());
        if (connectorKey.isEmpty()) {
          throw new IllegalArgumentException("Empty connector option key in: " + arg);
        }
        o.connectorOptions.put(connectorKey, value);
        continue;
      }
      if (!KNOWN_KEYS.contains(key)) {
        throw new IllegalArgumentException(
            "Unknown option --" + key + ". Known options: " + KNOWN_KEYS);
      }
      if (!seen.add(key)) {
        throw new IllegalArgumentException("Option --" + key + " given more than once");
      }
      o.set(key, value);
    }
    o.validate();
    return o;
  }

  private void set(String key, String value) {
    switch (key) {
      case "endpoints":
        endpoints = value.trim();
        break;
      case "database":
        database = value;
        break;
      case "user":
        user = value;
        break;
      case "passwordFile":
        passwordFile = emptyToNull(value);
        break;
      case "passwordEnv":
        passwordEnv = emptyToNull(value);
        break;
      case "password":
        password = value;
        break;
      case "protocol":
        protocol = value.toLowerCase(Locale.ROOT);
        break;
      case "sslEnabled":
        sslEnabled = parseBoolean(key, value);
        break;
      case "sslCertValue":
        sslCertValue = emptyToNull(value);
        break;
      case "sslTrustStorePath":
        sslTrustStorePath = emptyToNull(value);
        break;
      case "sslTrustStorePassword":
        sslTrustStorePassword = value;
        break;
      case "sslVerifyHost":
        sslVerifyHost = parseBoolean(key, value);
        break;
      case "acquireHostList":
        acquireHostList = parseBoolean(key, value);
        break;
      case "collections":
        collections = splitList(value);
        break;
      case "excludeCollections":
        excludeCollections = splitList(value);
        break;
      case "output":
        output = value.trim();
        break;
      case "exportId":
        exportId = value.trim();
        break;
      case "partitioning":
        try {
          partitioning = PartitioningMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "--partitioning must be RANGE or HASH but was: " + value, e);
        }
        break;
      case "partitions":
        partitions = (int) parseLong(key, value, 0, Integer.MAX_VALUE);
        break;
      case "targetDocsPerPartition":
        targetDocsPerPartition = parseLong(key, value, 1, Long.MAX_VALUE);
        break;
      case "maxPartitions":
        maxPartitions = (int) parseLong(key, value, 1, Integer.MAX_VALUE);
        break;
      case "rangesPerWrite":
        rangesPerWrite = (int) parseLong(key, value, 1, Integer.MAX_VALUE);
        break;
      case "parallelWrites":
        parallelWrites = (int) parseLong(key, value, 1, 1024);
        break;
      case "batchSize":
        batchSize = (int) parseLong(key, value, 1, Integer.MAX_VALUE);
        break;
      case "cursorTtlSeconds":
        cursorTtlSeconds = (int) parseLong(key, value, 1, Integer.MAX_VALUE);
        break;
      case "maxRecordsPerFile":
        maxRecordsPerFile = parseLong(key, value, 1, Long.MAX_VALUE);
        break;
      case "compression":
        compression = value.toLowerCase(Locale.ROOT);
        break;
      case "parallelCollections":
        parallelCollections = (int) parseLong(key, value, 1, 1024);
        break;
      case "ddlEdgeHelpers":
        ddlEdgeHelpers = parseBoolean(key, value);
        break;
      case "allowLiveSource":
        allowLiveSource = parseBoolean(key, value);
        break;
      case "verifyPartitions":
        verifyPartitions = parseBoolean(key, value);
        break;
      case "verifyRangePlans":
        verifyRangePlans = parseBoolean(key, value);
        break;
      case "planOnly":
        planOnly = parseBoolean(key, value);
        break;
      default:
        throw new IllegalArgumentException("Unknown option --" + key);
    }
  }

  private void validate() {
    if (endpoints == null || endpoints.isEmpty()) {
      throw new IllegalArgumentException("--endpoints is required (host:port[,host:port...])");
    }
    for (String endpoint : endpointList()) {
      parseEndpoint(endpoint);
    }
    if (output == null || output.isEmpty()) {
      throw new IllegalArgumentException("--output is required (e.g. gs://bucket/exports/db)");
    }
    while (output.endsWith("/") && !output.endsWith("://")) {
      output = output.substring(0, output.length() - 1);
    }
    if (exportId == null || exportId.isEmpty()) {
      exportId = EXPORT_ID_FORMAT.format(Instant.now());
    }
    if (!EXPORT_ID.matcher(exportId).matches()) {
      throw new IllegalArgumentException(
          "--exportId must match " + EXPORT_ID.pattern() + " but was: " + exportId);
    }
    if (!PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException("--protocol must be one of " + PROTOCOLS);
    }
    if (!COMPRESSIONS.contains(compression)) {
      throw new IllegalArgumentException("--compression must be one of " + COMPRESSIONS);
    }
    if (database == null || database.isEmpty()) {
      throw new IllegalArgumentException("--database must not be empty");
    }
  }

  /** Splits {@code host:port}. Exposed for the metadata client. */
  static String[] parseEndpoint(String endpoint) {
    int colon = endpoint.lastIndexOf(':');
    if (colon <= 0 || colon == endpoint.length() - 1) {
      throw new IllegalArgumentException("Endpoint must be host:port but was: " + endpoint);
    }
    String port = endpoint.substring(colon + 1);
    try {
      int p = Integer.parseInt(port);
      if (p <= 0 || p > 65535) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port in endpoint: " + endpoint, e);
    }
    return new String[] {endpoint.substring(0, colon), port};
  }

  private static String emptyToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  private static boolean parseBoolean(String key, String value) {
    String v = value.trim().toLowerCase(Locale.ROOT);
    if (v.equals("true")) {
      return true;
    }
    if (v.equals("false")) {
      return false;
    }
    throw new IllegalArgumentException("--" + key + " must be true or false but was: " + value);
  }

  private static long parseLong(String key, String value, long min, long max) {
    long v;
    try {
      v = Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("--" + key + " must be an integer but was: " + value, e);
    }
    if (v < min || v > max) {
      throw new IllegalArgumentException(
          "--" + key + " must be in [" + min + ", " + max + "] but was: " + value);
    }
    return v;
  }

  private static List<String> splitList(String value) {
    return Collections.unmodifiableList(
        Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList()));
  }

  /**
   * Number of partitions (AQL queries) for a collection with {@code count} documents: the explicit
   * {@code --partitions}, or {@code ceil(count / targetDocsPerPartition)} clamped to {@code [1,
   * maxPartitions]}. For HASH partitioning the automatic value is additionally capped at {@value
   * #HASH_MAX_AUTO_PARTITIONS} because every HASH partition is a full collection scan.
   */
  public int partitionsFor(long count) {
    if (partitions > 0) {
      return partitions;
    }
    long n = count <= 0 ? 1 : (count + targetDocsPerPartition - 1) / targetDocsPerPartition;
    long cap = maxPartitions;
    if (partitioning == PartitioningMode.HASH) {
      cap = Math.min(cap, HASH_MAX_AUTO_PARTITIONS);
    }
    return (int) Math.max(1, Math.min(n, cap));
  }

  /** {@code <output>/<exportId>}. */
  public String exportRoot() {
    return output + "/" + exportId;
  }

  /** Endpoints as a list of {@code host:port}. */
  public List<String> endpointList() {
    List<String> list = new ArrayList<>();
    for (String e : endpoints.split(",")) {
      if (!e.trim().isEmpty()) {
        list.add(e.trim());
      }
    }
    return list;
  }

  /**
   * Spark options for {@code format("com.arangodb.spark")}. The {@code query} option is added per
   * partition by {@link CollectionExporter}.
   */
  public Map<String, String> connectorBaseOptions(String resolvedPassword) {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("endpoints", String.join(",", endpointList()));
    m.put("database", database);
    m.put("user", user);
    m.put("password", resolvedPassword == null ? "" : resolvedPassword);
    m.put("protocol", protocol);
    m.put("contentType", "json");
    m.put("batchSize", Integer.toString(batchSize));
    m.put("stream", "true");
    // The connector's "ttl" is an int number of seconds (ArangoDBConf.ttlConf is an intConf).
    m.put("ttl", Integer.toString(cursorTtlSeconds));
    m.put("mode", "FAILFAST");
    m.put("acquireHostList", Boolean.toString(acquireHostList));
    if (sslEnabled) {
      m.put("ssl.enabled", "true");
      m.put("ssl.verifyHost", Boolean.toString(sslVerifyHost));
      if (sslCertValue != null) {
        m.put("ssl.cert.value", sslCertValue);
      }
      if (sslTrustStorePath != null) {
        m.put("ssl.trustStore.path", sslTrustStorePath);
      }
      if (sslTrustStorePassword != null) {
        m.put("ssl.trustStore.password", sslTrustStorePassword);
      }
    }
    m.putAll(connectorOptions);
    return m;
  }

  public String endpoints() {
    return endpoints;
  }

  public String database() {
    return database;
  }

  public String user() {
    return user;
  }

  public String passwordFile() {
    return passwordFile;
  }

  public String passwordEnv() {
    return passwordEnv;
  }

  public String password() {
    return password;
  }

  public String protocol() {
    return protocol;
  }

  public boolean sslEnabled() {
    return sslEnabled;
  }

  public String sslCertValue() {
    return sslCertValue;
  }

  public String sslTrustStorePath() {
    return sslTrustStorePath;
  }

  public String sslTrustStorePassword() {
    return sslTrustStorePassword;
  }

  public boolean sslVerifyHost() {
    return sslVerifyHost;
  }

  public boolean acquireHostList() {
    return acquireHostList;
  }

  public List<String> collections() {
    return collections;
  }

  public List<String> excludeCollections() {
    return excludeCollections;
  }

  public String output() {
    return output;
  }

  public String exportId() {
    return exportId;
  }

  public PartitioningMode partitioning() {
    return partitioning;
  }

  public int partitions() {
    return partitions;
  }

  public long targetDocsPerPartition() {
    return targetDocsPerPartition;
  }

  public int maxPartitions() {
    return maxPartitions;
  }

  public int rangesPerWrite() {
    return rangesPerWrite;
  }

  public int parallelWrites() {
    return parallelWrites;
  }

  public int batchSize() {
    return batchSize;
  }

  public int cursorTtlSeconds() {
    return cursorTtlSeconds;
  }

  public long maxRecordsPerFile() {
    return maxRecordsPerFile;
  }

  public String compression() {
    return compression;
  }

  public int parallelCollections() {
    return parallelCollections;
  }

  public boolean ddlEdgeHelpers() {
    return ddlEdgeHelpers;
  }

  public boolean allowLiveSource() {
    return allowLiveSource;
  }

  /**
   * Whether to count every planned partition with an index-only AQL {@code COLLECT WITH COUNT}
   * (same FILTER text as the export query) and require the sum to equal the collection count.
   */
  public boolean verifyPartitions() {
    return verifyPartitions;
  }

  /**
   * Whether to explain the first, a middle and the last RANGE query of every collection and fail
   * unless each is a pure primary-index range scan (see {@link RangePlanChecker}).
   */
  public boolean verifyRangePlans() {
    return verifyRangePlans;
  }

  /** Plan (and optionally verify) partitions, then stop without exporting anything. */
  public boolean planOnly() {
    return planOnly;
  }

  public Map<String, String> connectorOptions() {
    return Collections.unmodifiableMap(connectorOptions);
  }

  @Override
  public String toString() {
    Map<String, String> redactedConnector = new LinkedHashMap<>();
    connectorOptions.forEach(
        (k, v) ->
            redactedConnector.put(
                k, k.toLowerCase(Locale.ROOT).contains("password") ? "<redacted>" : v));
    return "ExporterOptions{"
        + "endpoints="
        + endpoints
        + ", database="
        + database
        + ", user="
        + user
        + ", passwordFile="
        + passwordFile
        + ", passwordEnv="
        + passwordEnv
        + ", password="
        + (password == null ? "null" : "<redacted>")
        + ", protocol="
        + protocol
        + ", sslEnabled="
        + sslEnabled
        + ", sslCertValue="
        + (sslCertValue == null ? "null" : "<set>")
        + ", sslTrustStorePath="
        + sslTrustStorePath
        + ", sslTrustStorePassword="
        + (sslTrustStorePassword == null ? "null" : "<redacted>")
        + ", sslVerifyHost="
        + sslVerifyHost
        + ", acquireHostList="
        + acquireHostList
        + ", collections="
        + collections
        + ", excludeCollections="
        + excludeCollections
        + ", output="
        + output
        + ", exportId="
        + exportId
        + ", partitioning="
        + partitioning
        + ", partitions="
        + partitions
        + ", targetDocsPerPartition="
        + targetDocsPerPartition
        + ", maxPartitions="
        + maxPartitions
        + ", rangesPerWrite="
        + rangesPerWrite
        + ", parallelWrites="
        + parallelWrites
        + ", batchSize="
        + batchSize
        + ", cursorTtlSeconds="
        + cursorTtlSeconds
        + ", maxRecordsPerFile="
        + maxRecordsPerFile
        + ", compression="
        + compression
        + ", parallelCollections="
        + parallelCollections
        + ", ddlEdgeHelpers="
        + ddlEdgeHelpers
        + ", allowLiveSource="
        + allowLiveSource
        + ", verifyPartitions="
        + verifyPartitions
        + ", verifyRangePlans="
        + verifyRangePlans
        + ", planOnly="
        + planOnly
        + ", connectorOptions="
        + redactedConnector
        + '}';
  }
}
