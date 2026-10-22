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
package com.google.cloud.teleport.v2.templates.debezium;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads Debezium change events from Kafka and emits them as Datastream-shaped JSON records.
 *
 * <p>Output {@link #CHANGE_EVENTS} has the same element type and payload contract as {@code
 * DataStreamIO}, so it can be flattened with DLQ retries and fed to the existing transformation and
 * Spanner writer stages. Records that cannot be parsed go to {@link #PERMANENT_ERRORS} with the raw
 * value as payload and the parse error as the error message.
 */
public class ReadDebeziumChangeEvents extends PTransform<PBegin, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadDebeziumChangeEvents.class);

  public static final TupleTag<FailsafeElement<String, String>> CHANGE_EVENTS =
      new TupleTag<FailsafeElement<String, String>>() {};
  public static final TupleTag<FailsafeElement<String, String>> PERMANENT_ERRORS =
      new TupleTag<FailsafeElement<String, String>>() {};

  private final String bootstrapServers;
  private final String topics;
  private final String topicPattern;
  private final String consumerGroupId;
  private final String startOffset;
  private final String consumerConfig;
  private final String sourceType;

  public ReadDebeziumChangeEvents(
      String bootstrapServers,
      String topics,
      String topicPattern,
      String consumerGroupId,
      String startOffset,
      String consumerConfig,
      String sourceType) {
    if (Strings.isNullOrEmpty(bootstrapServers)) {
      throw new IllegalArgumentException(
          "kafkaBootstrapServers is required for cdcSource=debezium");
    }
    if (Strings.isNullOrEmpty(topics) == Strings.isNullOrEmpty(topicPattern)) {
      throw new IllegalArgumentException(
          "Specify exactly one of kafkaTopics or kafkaTopicPattern for cdcSource=debezium");
    }
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.topicPattern = topicPattern;
    this.consumerGroupId = consumerGroupId;
    this.startOffset = Strings.isNullOrEmpty(startOffset) ? "earliest" : startOffset;
    this.consumerConfig = consumerConfig;
    this.sourceType = sourceType;
  }

  /** Parses {@code key1=value1;key2=value2} into Kafka consumer properties. */
  static Map<String, Object> parseConsumerConfig(String config) {
    Map<String, Object> props = new HashMap<>();
    if (Strings.isNullOrEmpty(config)) {
      return props;
    }
    for (String entry : config.split(";")) {
      if (entry.isBlank()) {
        continue;
      }
      int eq = entry.indexOf('=');
      if (eq <= 0) {
        throw new IllegalArgumentException("Invalid kafkaConsumerConfig entry: " + entry);
      }
      props.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
    }
    return props;
  }

  @Override
  public PCollectionTuple expand(PBegin input) {
    Map<String, Object> props = new HashMap<>();
    props.put("auto.offset.reset", startOffset);
    if (!Strings.isNullOrEmpty(consumerGroupId)) {
      props.put("group.id", consumerGroupId);
    }
    props.putAll(parseConsumerConfig(consumerConfig));

    KafkaIO.Read<byte[], byte[]> read =
        KafkaIO.<byte[], byte[]>read()
            .withBootstrapServers(bootstrapServers)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .withConsumerConfigUpdates(props);
    if (!Strings.isNullOrEmpty(topics)) {
      List<String> topicList =
          Arrays.stream(topics.split(","))
              .map(String::trim)
              .filter(t -> !t.isEmpty())
              .collect(Collectors.toList());
      read = read.withTopics(topicList);
    } else {
      read = read.withTopicPattern(topicPattern);
    }
    if (!Strings.isNullOrEmpty(consumerGroupId)) {
      // Offsets are tracked by the runner's checkpoints; committing them as well makes consumer lag
      // visible to standard Kafka tooling.
      read = read.commitOffsetsInFinalize();
    }

    PCollectionTuple out =
        input
            .apply("Read Debezium events from Kafka", read)
            .apply(
                "Debezium to Datastream JSON",
                ParDo.of(new MapFn(sourceType))
                    .withOutputTags(CHANGE_EVENTS, TupleTagList.of(PERMANENT_ERRORS)));
    FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    out.get(CHANGE_EVENTS).setCoder(coder);
    out.get(PERMANENT_ERRORS).setCoder(coder);
    return out;
  }

  static class MapFn extends DoFn<KafkaRecord<byte[], byte[]>, FailsafeElement<String, String>> {
    private static final Pattern INTERNAL_TOPIC = Pattern.compile("^__.*");

    private final DebeziumChangeEventMapper mapper;
    private final Counter changes = Metrics.counter(MapFn.class, "Debezium change events");
    private final Counter ignored = Metrics.counter(MapFn.class, "Debezium ignored records");
    private final Counter errors = Metrics.counter(MapFn.class, "Debezium parse errors");

    MapFn(String sourceType) {
      this.mapper = new DebeziumChangeEventMapper(sourceType);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KafkaRecord<byte[], byte[]> record = c.element();
      if (INTERNAL_TOPIC.matcher(record.getTopic()).matches()) {
        ignored.inc();
        return;
      }
      byte[] value = record.getKV().getValue();
      String seed = record.getTopic() + "/" + record.getPartition() + "/" + record.getOffset();
      try {
        DebeziumChangeEventMapper.Result result = mapper.map(record.getKV().getKey(), value, seed);
        if (result.kind() == DebeziumChangeEventMapper.Kind.IGNORED) {
          ignored.inc();
          return;
        }
        String json = result.event().toString();
        changes.inc();
        c.output(CHANGE_EVENTS, FailsafeElement.of(json, json));
      } catch (Exception e) {
        errors.inc();
        String raw = value == null ? "" : new String(value, StandardCharsets.UTF_8);
        LOG.error("Could not map Debezium record {}: {}", seed, e.getMessage());
        c.output(
            PERMANENT_ERRORS,
            FailsafeElement.of(raw, raw)
                .setErrorMessage("Debezium record " + seed + ": " + e.getMessage()));
      }
    }
  }
}
