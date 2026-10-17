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

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.arangodb.entity.AqlQueryExplainEntity;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.CollectionsReadOptions;
import com.arangodb.model.ExplainAqlQueryOptions;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Driver-side ArangoDB access (collection list, counts, revisions, key sampling) through the shaded
 * Java driver. Thread-safe; one instance is shared by all collection tasks.
 */
public final class ArangoMetadataClient implements AutoCloseable, KeyRangePlanner.KeySource {

  /** Lower bound for the cursor batch size of metadata queries (small result values). */
  static final int METADATA_MIN_BATCH_SIZE = 10_000;

  private final ArangoDB arango;
  private final ArangoDatabase db;
  private final ExporterOptions opts;

  public ArangoMetadataClient(ExporterOptions opts, String password) {
    this.opts = opts;
    ArangoDB.Builder builder =
        new ArangoDB.Builder()
            .user(opts.user())
            .password(password == null ? "" : password)
            .protocol(toDriverProtocol(opts.protocol()))
            .acquireHostList(opts.acquireHostList());
    for (String endpoint : opts.endpointList()) {
      String[] hostPort = ExporterOptions.parseEndpoint(endpoint);
      builder.host(hostPort[0], Integer.parseInt(hostPort[1]));
    }
    if (opts.sslEnabled()) {
      builder.useSsl(true).verifyHost(opts.sslVerifyHost());
      SSLContext ctx = sslContext(opts);
      if (ctx != null) {
        builder.sslContext(ctx);
      }
    }
    this.arango = builder.build();
    this.db = arango.db(opts.database());
  }

  static Protocol toDriverProtocol(String protocol) {
    switch (protocol) {
      case "http":
        return Protocol.HTTP_JSON;
      case "http2":
        return Protocol.HTTP2_JSON;
      case "vst":
        return Protocol.VST;
      default:
        throw new IllegalArgumentException("Unsupported protocol " + protocol);
    }
  }

  private static SSLContext sslContext(ExporterOptions opts) {
    try {
      KeyStore ks = null;
      if (opts.sslCertValue() != null) {
        ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        byte[] der = Base64.getDecoder().decode(opts.sslCertValue().trim());
        Certificate cert =
            CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(der));
        ks.setCertificateEntry("arangodb", cert);
      } else if (opts.sslTrustStorePath() != null) {
        ks = KeyStore.getInstance(KeyStore.getDefaultType());
        char[] pw =
            opts.sslTrustStorePassword() == null
                ? null
                : opts.sslTrustStorePassword().toCharArray();
        try (InputStream in = new FileInputStream(opts.sslTrustStorePath())) {
          ks.load(in, pw);
        }
      }
      if (ks == null) {
        return null; // JVM default trust store
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, tmf.getTrustManagers(), null);
      return ctx;
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalArgumentException("Cannot build the SSL context for ArangoDB", e);
    }
  }

  /** Server version, e.g. {@code 3.11.14}. */
  public String serverVersion() {
    return arango.getVersion().getVersion();
  }

  /** Non-system collections of the database, sorted by name. */
  public List<CollectionInfo> listCollections() {
    List<CollectionInfo> result = new ArrayList<>();
    for (CollectionEntity e : db.getCollections(new CollectionsReadOptions().excludeSystem(true))) {
      if (Boolean.TRUE.equals(e.getIsSystem()) || e.getName().startsWith("_")) {
        continue;
      }
      CollectionKind kind =
          e.getType() == CollectionType.EDGES ? CollectionKind.EDGE : CollectionKind.DOCUMENT;
      result.add(new CollectionInfo(e.getName(), kind));
    }
    result.sort(Comparator.comparing(CollectionInfo::sourceName));
    return result;
  }

  /** Document count. */
  public long count(String collection) {
    return db.collection(collection).count().getCount();
  }

  /** Collection revision; changes on every write. */
  public String revision(String collection) {
    return db.collection(collection).getRevision().getRevision();
  }

  private AqlQueryOptions streamOptions() {
    return new AqlQueryOptions()
        .stream(true)
            // Metadata queries return small values (keys, counts, booleans): use bigger batches.
            .batchSize(Math.max(opts.batchSize(), METADATA_MIN_BATCH_SIZE))
            .ttl(opts.cursorTtlSeconds());
  }

  @Override
  public List<String> sampleKeys(String collection, double probability) {
    Map<String, Object> bind = new HashMap<>();
    bind.put("@c", collection);
    String query;
    if (probability >= 1.0) {
      query = "FOR d IN @@c RETURN d._key";
    } else {
      query = "FOR d IN @@c FILTER RAND() < @p RETURN d._key";
      bind.put("p", probability);
    }
    return collect(query, bind);
  }

  @Override
  public List<String> collationSort(List<String> values) {
    Map<String, Object> bind = new HashMap<>();
    bind.put("b", values);
    return collect("FOR b IN @b SORT b RETURN b", bind);
  }

  @Override
  public List<Boolean> collationLessThanPairs(List<String> values) {
    if (values.size() < 2) {
      return new ArrayList<>();
    }
    Map<String, Object> bind = new HashMap<>();
    bind.put("b", values);
    bind.put("last", values.size() - 2);
    List<Boolean> out = new ArrayList<>();
    try (ArangoCursor<Boolean> cursor =
        db.query(
            "FOR i IN 0..@last RETURN @b[i] < @b[i + 1]", Boolean.class, bind, streamOptions())) {
      cursor.forEach(out::add);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot close AQL cursor", e);
    }
    return out;
  }

  /** Runs a query returning a single number (e.g. {@link AqlBuilder#countQuery}). */
  public long countQuery(String aql) {
    try (ArangoCursor<Long> cursor = db.query(aql, Long.class, new HashMap<>(), streamOptions())) {
      long total = 0;
      while (cursor.hasNext()) {
        Long v = cursor.next();
        total += v == null ? 0 : v;
      }
      return total;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot close AQL cursor", e);
    }
  }

  private List<String> collect(String query, Map<String, Object> bind) {
    List<String> out = new ArrayList<>();
    try (ArangoCursor<String> cursor = db.query(query, String.class, bind, streamOptions())) {
      cursor.forEach(out::add);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot close AQL cursor", e);
    }
    return out;
  }

  /**
   * Explains {@code aql} (without running it) and returns the plan nodes as {@code {type, indexes}}
   * maps for {@link RangePlanChecker}.
   */
  public List<Map<String, Object>> explainNodes(String aql) {
    AqlQueryExplainEntity explain =
        db.explainAqlQuery(aql, new HashMap<>(), new ExplainAqlQueryOptions());
    List<Map<String, Object>> nodes = new ArrayList<>();
    for (AqlQueryExplainEntity.ExecutionNode n : explain.getPlan().getNodes()) {
      Map<String, Object> m = new HashMap<>();
      m.put("type", n.get("type"));
      m.put("indexes", n.get("indexes"));
      m.put("filter", n.get("filter"));
      nodes.add(m);
    }
    return nodes;
  }

  /** The underlying database handle (tests). */
  ArangoDatabase database() {
    return db;
  }

  @Override
  public void close() {
    arango.shutdown();
  }
}
