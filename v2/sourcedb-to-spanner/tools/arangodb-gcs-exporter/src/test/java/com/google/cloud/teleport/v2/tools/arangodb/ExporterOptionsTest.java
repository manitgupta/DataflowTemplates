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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExporterOptionsTest {

  private static ExporterOptions parse(String... extra) {
    String[] base = {"--endpoints=h1:8529,h2:8529", "--output=gs://b/exports/"};
    String[] all = new String[base.length + extra.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(extra, 0, all, base.length, extra.length);
    return ExporterOptions.parse(all);
  }

  @Test
  public void requiredOptions() {
    assertThrows(
        IllegalArgumentException.class, () -> ExporterOptions.parse(new String[] {"--output=x"}));
    assertThrows(
        IllegalArgumentException.class,
        () -> ExporterOptions.parse(new String[] {"--endpoints=h:1"}));
  }

  @Test
  public void defaults() {
    ExporterOptions o = parse();
    assertThat(o.database()).isEqualTo("_system");
    assertThat(o.user()).isEqualTo("root");
    assertThat(o.passwordEnv()).isEqualTo("ARANGO_PASSWORD");
    assertThat(o.protocol()).isEqualTo("http2");
    assertThat(o.partitioning()).isEqualTo(PartitioningMode.RANGE);
    assertThat(o.partitions()).isEqualTo(0);
    assertThat(o.targetDocsPerPartition()).isEqualTo(2_000_000L);
    assertThat(o.maxPartitions()).isEqualTo(20_000);
    assertThat(o.rangesPerWrite()).isEqualTo(64);
    assertThat(o.parallelWrites()).isEqualTo(8);
    assertThat(o.batchSize()).isEqualTo(1_000);
    assertThat(o.verifyRangePlans()).isTrue();
    assertThat(o.cursorTtlSeconds()).isEqualTo(600);
    assertThat(o.maxRecordsPerFile()).isEqualTo(2_000_000L);
    assertThat(o.compression()).isEqualTo("snappy");
    assertThat(o.parallelCollections()).isEqualTo(1);
    assertThat(o.ddlEdgeHelpers()).isTrue();
    assertThat(o.allowLiveSource()).isFalse();
    assertThat(o.sslEnabled()).isFalse();
    assertThat(o.verifyPartitions()).isFalse();
    assertThat(o.planOnly()).isFalse();
    assertThat(o.output()).isEqualTo("gs://b/exports");
    assertThat(o.exportId()).matches("\\d{8}T\\d{6}Z");
    assertThat(o.exportRoot()).isEqualTo("gs://b/exports/" + o.exportId());
    assertThat(o.endpointList()).containsExactly("h1:8529", "h2:8529").inOrder();
  }

  @Test
  public void unknownKeyThrows() {
    assertThrows(IllegalArgumentException.class, () -> parse("--nope=1"));
    assertThrows(IllegalArgumentException.class, () -> parse("positional"));
    assertThrows(IllegalArgumentException.class, () -> parse("--database=a", "--database=b"));
  }

  @Test
  public void badValuesThrow() {
    assertThrows(IllegalArgumentException.class, () -> parse("--exportId=a/b"));
    assertThrows(IllegalArgumentException.class, () -> parse("--exportId=a b"));
    assertThrows(IllegalArgumentException.class, () -> parse("--partitions=-1"));
    assertThrows(IllegalArgumentException.class, () -> parse("--partitions=x"));
    assertThrows(IllegalArgumentException.class, () -> parse("--protocol=grpc"));
    assertThrows(IllegalArgumentException.class, () -> parse("--compression=zstd"));
    assertThrows(IllegalArgumentException.class, () -> parse("--sslEnabled=yes"));
    assertThrows(IllegalArgumentException.class, () -> parse("--partitioning=RANDOM"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ExporterOptions.parse(new String[] {"--endpoints=hostonly", "--output=x"}));
  }

  @Test
  public void connectorOptionsPassThroughAndOverride() {
    ExporterOptions o =
        parse(
            "--connectorOption.timeout=900000",
            "--connectorOption.batchSize=5",
            "--password=s3cret",
            "--cursorTtlSeconds=1200");
    assertThat(o.connectorOptions()).containsExactly("timeout", "900000", "batchSize", "5");
    Map<String, String> m = o.connectorBaseOptions("pw");
    assertThat(m).containsEntry("endpoints", "h1:8529,h2:8529");
    assertThat(m).containsEntry("database", "_system");
    assertThat(m).containsEntry("user", "root");
    assertThat(m).containsEntry("password", "pw");
    assertThat(m).containsEntry("protocol", "http2");
    assertThat(m).containsEntry("contentType", "json");
    assertThat(m).containsEntry("stream", "true");
    assertThat(m).containsEntry("ttl", "1200");
    assertThat(m).containsEntry("mode", "FAILFAST");
    assertThat(m).containsEntry("acquireHostList", "false");
    assertThat(m).containsEntry("timeout", "900000");
    assertThat(m).containsEntry("batchSize", "5");
    assertThat(m).doesNotContainKey("ssl.enabled");
  }

  @Test
  public void sslOptions() {
    ExporterOptions o =
        parse(
            "--sslEnabled=true",
            "--sslCertValue=QUJD",
            "--sslVerifyHost=false",
            "--sslTrustStorePassword=tspw");
    Map<String, String> m = o.connectorBaseOptions("");
    assertThat(m).containsEntry("ssl.enabled", "true");
    assertThat(m).containsEntry("ssl.cert.value", "QUJD");
    assertThat(m).containsEntry("ssl.verifyHost", "false");
    assertThat(m).containsEntry("ssl.trustStore.password", "tspw");
    assertThat(m).doesNotContainKey("ssl.trustStore.path");
  }

  @Test
  public void passwordRedacted() {
    ExporterOptions o =
        parse(
            "--password=s3cret",
            "--sslTrustStorePassword=tspw",
            "--connectorOption.ssl.trustStore.password=cpw");
    String s = o.toString();
    assertThat(s).doesNotContain("s3cret");
    assertThat(s).doesNotContain("tspw");
    assertThat(s).doesNotContain("cpw");
    assertThat(s).contains("<redacted>");
  }

  @Test
  public void autoPartitions() {
    ExporterOptions o = parse();
    assertThat(o.partitionsFor(0)).isEqualTo(1);
    assertThat(o.partitionsFor(1)).isEqualTo(1);
    assertThat(o.partitionsFor(2_000_000)).isEqualTo(1);
    assertThat(o.partitionsFor(2_500_000)).isEqualTo(2);
    assertThat(o.partitionsFor(1_000_000_000L)).isEqualTo(500);
    assertThat(o.partitionsFor(100_000_000_000L)).isEqualTo(20_000);

    ExporterOptions small = parse("--targetDocsPerPartition=1000000", "--maxPartitions=64");
    assertThat(small.partitionsFor(0)).isEqualTo(1);
    assertThat(small.partitionsFor(2_500_000)).isEqualTo(3);
    assertThat(small.partitionsFor(1_000_000_000L)).isEqualTo(64);

    ExporterOptions hash = parse("--partitioning=hash");
    assertThat(hash.partitioning()).isEqualTo(PartitioningMode.HASH);
    assertThat(hash.partitionsFor(1_000_000_000L))
        .isEqualTo(ExporterOptions.HASH_MAX_AUTO_PARTITIONS);

    assertThat(parse("--partitions=7").partitionsFor(1_000_000_000L)).isEqualTo(7);
  }

  @Test
  public void listsAndFlags() {
    ExporterOptions o =
        parse(
            "--collections= a, b ,,c",
            "--excludeCollections=b",
            "--exportId=e2e_1-x",
            "--allowLiveSource=TRUE",
            "--ddlEdgeHelpers=false",
            "--protocol=VST",
            "--verifyPartitions=true",
            "--planOnly=true");
    assertThat(o.verifyPartitions()).isTrue();
    assertThat(o.planOnly()).isTrue();
    assertThat(o.collections()).containsExactly("a", "b", "c").inOrder();
    assertThat(o.excludeCollections()).containsExactly("b");
    assertThat(o.exportId()).isEqualTo("e2e_1-x");
    assertThat(o.allowLiveSource()).isTrue();
    assertThat(o.ddlEdgeHelpers()).isFalse();
    assertThat(o.protocol()).isEqualTo("vst");
  }
}
