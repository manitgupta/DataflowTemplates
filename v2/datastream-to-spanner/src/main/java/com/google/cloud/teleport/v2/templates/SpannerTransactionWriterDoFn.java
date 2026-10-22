/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.SHARD_ID_COLUMN_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.CONVERSION_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.DROPPED_TABLE_EXCEPTIONS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.OTHER_PERMANENT_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SKIPPED_EVENTS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SUCCESSFUL_EVENTS_COUNTER_NAME;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerExceptionParser;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.source.DatastreamToSpannerSourceConnectorRegistry;
import com.google.cloud.teleport.v2.templates.source.IDsToSpSourceConnector;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag;
import com.google.cloud.teleport.v2.templates.utils.WatchdogRunnable;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Change events from DataStream into Cloud Spanner.
 *
 * <p>Change events are individually processed. Shadow tables store the version information(that
 * specifies the commit order) for each primary key. Shadow tables are consulted before actual
 * writes to Cloud Spanner to preserve the correctness and consistency of data.
 *
 * <p>Change events written successfully will be pushed onto the primary output with their commit
 * timestamps.
 *
 * <p>Change events that failed to be written will be pushed onto the secondary output tagged with
 * PERMANENT_ERROR_TAG/RETRYABLE_ERROR_TAG along with the exception that caused the failure.
 */
class SpannerTransactionWriterDoFn
    extends DoFn<KV<Long, FailsafeElement<String, String>>, Timestamp> implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTransactionWriterDoFn.class);

  private final PCollectionView<Ddl> ddlView;
  private final PCollectionView<Ddl> shadowTableDdlView;

  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  private transient IDsToSpSourceConnector sourceConnector;
  /* SpannerAccessor for shadow table database must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor shadowTableSpannerAccessor;

  // Number of events successfully processed.
  private final Counter successfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SUCCESSFUL_EVENTS_COUNTER_NAME);

  // Number of events that have a schema incompatible with the spanner schema.
  private final Counter invalidEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Invalid events");

  // Number of events skipped from being written to spanner because the events were stale.
  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SKIPPED_EVENTS_COUNTER_NAME);

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, OTHER_PERMANENT_ERRORS_COUNTER_NAME);

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, CONVERSION_ERRORS_COUNTER_NAME);

  private final Counter retryableErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, RETRYABLE_ERRORS_COUNTER_NAME);

  // Number of events that were successfully read from dlq/retry and written to spanner.
  private final Counter successfulEventRetries =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Successful event retries");

  // Distribution of retries done for any event.
  private final Distribution eventRetries =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Event retries");

  // Time taken from events being read by Datastream to being written to Cloud Spanner. Time
  // duration between datastream_read_timestamp and write_timestamp
  private final Distribution systemLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag system latency");

  // Time taken for events to get processed by the Dataflow pipeline. Time duration between
  // dataflow_read_timestamp and write_timestamp.
  private final Distribution dataflowLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag dataflow latency");

  // Time duration between source_timestamp and write_timestamp.
  private final Distribution totalLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag total latency");

  // Latency of creating and writing mutations from change events to spanner.
  private final Distribution spannerWriterLatencyMs =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "spanner_writer_latency_ms");

  private final Counter batchedTransactions =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Batched transactions");

  private final Distribution batchedEvents =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Events per batched transaction");

  private final Counter batchFallbacks =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Batched transaction fallbacks");

  private final Counter droppedTableExceptions =
      Metrics.counter(SpannerTransactionWriterDoFn.class, DROPPED_TABLE_EXCEPTIONS_COUNTER_NAME);

  // The max length of tag allowed in Spanner Transaction tags.
  private static final int MAX_TXN_TAG_LENGTH = 50;

  /* The run mode, whether it is regular or retry. */
  private final Boolean isRegularRunMode;

  /*
   * The watchdog thread monitors the progress of Spanner transactions and ensures that they
   * are not stuck for an extended period of time. This is important because in load testing there were
   * instances where transactions were stuck, causing bottlenecks in the Dataflow pipeline.
   *
   * The WatchdogRunnable is designed to track if a transaction is making progress by comparing
   * the number of transaction attempts (`transactionAttemptCount`) over time. The `isInTransaction`
   * flag indicates whether a transaction is currently active. If the number of attempts remains
   * the same for a period of 15 minutes while the transaction is active, the watchdog logs a warning
   * and terminates the process by calling `System.exit(1)`.
   *
   * By running in the background, this watchdog thread ensures that long-running transactions
   * do not stall indefinitely, providing a safeguard mechanism for transaction processing in
   * the pipeline.
   */
  private transient AtomicLong transactionAttemptCount;
  private transient AtomicBoolean isInTransaction;
  private transient AtomicBoolean keepWatchdogRunning;
  private transient Thread watchdogThread;
  /* Transactions currently in flight; isInTransaction mirrors (inFlightTransactions > 0). */
  private transient int inFlightTransactions;

  private final boolean usesSeparateShadowTableDb;

  /*
   * Spanner transactions in flight per DoFn instance. 1 (default) processes events one at a time on
   * the runner thread. Larger values run events on that many lanes; events are assigned to a lane by
   * primary-key hash, so changes to one row are still applied in arrival order, and the bundle only
   * finishes once every lane has drained. Runners that give each DoFn instance a single thread
   * (Flink: one per task slot) need this to reach the concurrency Dataflow gets from its many
   * threads per worker.
   */
  private final int writerConcurrency;

  /*
   * Events per Spanner transaction on each lane. 1 (default) commits every event on its own. Larger
   * values read the shadow rows of up to this many events with one request and commit them
   * together; the per-row ordering check is unchanged. Events that cannot be batched (separate
   * shadow table database, DML deletes, sources without batched sequence reads) and batches that
   * fail are applied one event per transaction, so error handling stays per event.
   */
  private final int writerBatchSize;

  private transient ExecutorService[] lanes;
  private transient List<PendingEvent>[] laneBatches;
  private transient BatchContext lastBatchContext;
  private transient List<Future<?>> pendingEvents;
  private transient ConcurrentLinkedQueue<BufferedOutput> bufferedOutputs;
  private transient String txnTag;

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode) {
    this(
        spannerConfig,
        shadowTableSpannerConfig,
        ddlView,
        shadowTableDdlView,
        shadowTablePrefix,
        sourceType,
        isRegularRunMode,
        1);
  }

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode,
      int writerConcurrency) {
    this(
        spannerConfig,
        shadowTableSpannerConfig,
        ddlView,
        shadowTableDdlView,
        shadowTablePrefix,
        sourceType,
        isRegularRunMode,
        writerConcurrency,
        1);
  }

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode,
      int writerConcurrency,
      int writerBatchSize) {
    Preconditions.checkNotNull(spannerConfig);
    Preconditions.checkArgument(writerConcurrency >= 1, "writerConcurrency must be >= 1");
    Preconditions.checkArgument(writerBatchSize >= 1, "writerBatchSize must be >= 1");
    this.writerConcurrency = writerConcurrency;
    this.writerBatchSize = writerBatchSize;
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.ddlView = ddlView;
    this.shadowTableDdlView = shadowTableDdlView;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
    this.isRegularRunMode = isRegularRunMode;
    this.usesSeparateShadowTableDb =
        !(spannerConfig.getInstanceId().equals(shadowTableSpannerConfig.getInstanceId())
            && spannerConfig.getDatabaseId().equals(shadowTableSpannerConfig.getDatabaseId()));
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    shadowTableSpannerAccessor =
        usesSeparateShadowTableDb
            ? SpannerAccessor.getOrCreate(shadowTableSpannerConfig)
            : spannerAccessor;
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceConnector = DatastreamToSpannerSourceConnectorRegistry.getSourceConnector(sourceType);
    // Setup and start the watchdog thread.
    transactionAttemptCount = new AtomicLong(0);
    isInTransaction = new AtomicBoolean(false);
    keepWatchdogRunning = new AtomicBoolean(true);
    watchdogThread =
        new Thread(
            new WatchdogRunnable(transactionAttemptCount, isInTransaction, keepWatchdogRunning),
            "SpannerTransactionWriterDoFn.WatchdogThread");
    watchdogThread.setDaemon(true);
    watchdogThread.start();
    startLanes();
  }

  /** Starts the per-instance lanes when writerConcurrency > 1 (separate from setup for tests). */
  void startLanes() {
    if ((writerConcurrency > 1 || writerBatchSize > 1) && lanes == null) {
      lanes = new ExecutorService[writerConcurrency];
      for (int i = 0; i < writerConcurrency; i++) {
        final int lane = i;
        lanes[i] =
            Executors.newSingleThreadExecutor(
                r -> {
                  Thread t = new Thread(r, "SpannerTransactionWriterDoFn.lane-" + lane);
                  t.setDaemon(true);
                  return t;
                });
      }
      pendingEvents = new ArrayList<>();
      bufferedOutputs = new ConcurrentLinkedQueue<>();
      @SuppressWarnings("unchecked")
      List<PendingEvent>[] batches = new List[writerConcurrency];
      for (int i = 0; i < writerConcurrency; i++) {
        batches[i] = new ArrayList<>();
      }
      laneBatches = batches;
    }
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerAccessor.close();
    if (usesSeparateShadowTableDb) {
      shadowTableSpannerAccessor.close();
    }
    // Stop the watchdog thread.
    keepWatchdogRunning.set(false);
    if (lanes != null) {
      for (ExecutorService lane : lanes) {
        lane.shutdownNow();
      }
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    KV<Long, FailsafeElement<String, String>> element = c.element();
    Ddl ddl = c.sideInput(ddlView);
    // TODO: pass shadow table ddl to shdaow tble mutaiton generator and sequence reader.
    Ddl shadowTableDdl = c.sideInput(shadowTableDdlView);
    String tag = getCachedTxnTag(c.getPipelineOptions());
    if (lanes == null) {
      processEvent(element.getValue(), ddl, shadowTableDdl, tag, new ContextOutput(c));
      return;
    }
    // Lane by primary-key hash: one row's changes stay ordered, different rows run concurrently.
    long key = element.getKey() == null ? 0L : element.getKey();
    int lane = (int) Math.floorMod(key, (long) lanes.length);
    Instant timestamp = c.timestamp();
    MetricsContainer metricsContainer = MetricsEnvironment.getCurrentContainer();
    if (writerBatchSize > 1) {
      lastBatchContext = new BatchContext(ddl, shadowTableDdl, tag, metricsContainer);
      List<PendingEvent> batch = laneBatches[lane];
      batch.add(
          new PendingEvent(
              element.getValue(), new BufferingOutput(bufferedOutputs, timestamp, window)));
      if (batch.size() >= writerBatchSize) {
        submitBatch(lane, ddl, shadowTableDdl, tag, metricsContainer);
      }
      return;
    }
    pendingEvents.add(
        lanes[lane].submit(
            () -> {
              try (Closeable ignored =
                  MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
                processEvent(
                    element.getValue(),
                    ddl,
                    shadowTableDdl,
                    tag,
                    new BufferingOutput(bufferedOutputs, timestamp, window));
              }
              return null;
            }));
  }

  /** Processes one element synchronously in the global window (used by tests). */
  public void processElement(ProcessContext c) {
    processElement(c, GlobalWindow.INSTANCE);
  }

  private void submitBatch(
      int lane, Ddl ddl, Ddl shadowTableDdl, String tag, MetricsContainer metricsContainer) {
    List<PendingEvent> batch = new ArrayList<>(laneBatches[lane]);
    laneBatches[lane].clear();
    pendingEvents.add(
        lanes[lane].submit(
            () -> {
              try (Closeable ignored =
                  MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
                processEventBatch(batch, ddl, shadowTableDdl, tag);
              }
              return null;
            }));
  }

  /** Waits for all lanes to drain, then emits the outputs buffered during the bundle. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    if (lanes == null) {
      return;
    }
    if (laneBatches != null && lastBatchContext != null) {
      for (int lane = 0; lane < laneBatches.length; lane++) {
        if (!laneBatches[lane].isEmpty()) {
          submitBatch(
              lane,
              lastBatchContext.ddl,
              lastBatchContext.shadowTableDdl,
              lastBatchContext.txnTag,
              lastBatchContext.metricsContainer);
        }
      }
    }
    Exception failure = null;
    for (Future<?> f : pendingEvents) {
      try {
        f.get();
      } catch (ExecutionException | InterruptedException e) {
        if (failure == null) {
          failure = e;
        }
      }
    }
    pendingEvents.clear();
    if (failure != null) {
      bufferedOutputs.clear();
      throw failure;
    }
    BufferedOutput o;
    while ((o = bufferedOutputs.poll()) != null) {
      if (o.tag == null) {
        context.output((com.google.cloud.Timestamp) o.value, o.timestamp, o.window);
      } else {
        @SuppressWarnings("unchecked")
        TupleTag<Object> tag = (TupleTag<Object>) o.tag;
        context.output(tag, o.value, o.timestamp, o.window);
      }
    }
  }

  /** Where the outcome of one event goes: the runner context, or a buffer flushed per bundle. */
  interface EventOutput {
    void success(com.google.cloud.Timestamp timestamp);

    void error(
        TupleTag<FailsafeElement<String, String>> tag, FailsafeElement<String, String> element);
  }

  private static final class ContextOutput implements EventOutput {
    private final ProcessContext c;

    ContextOutput(ProcessContext c) {
      this.c = c;
    }

    @Override
    public void success(com.google.cloud.Timestamp timestamp) {
      c.output(timestamp);
    }

    @Override
    public void error(
        TupleTag<FailsafeElement<String, String>> tag, FailsafeElement<String, String> element) {
      c.output(tag, element);
    }
  }

  private static final class BufferedOutput {
    final TupleTag<?> tag; // null = main output
    final Object value;
    final Instant timestamp;
    final BoundedWindow window;

    BufferedOutput(TupleTag<?> tag, Object value, Instant timestamp, BoundedWindow window) {
      this.tag = tag;
      this.value = value;
      this.timestamp = timestamp;
      this.window = window;
    }
  }

  private static final class BufferingOutput implements EventOutput {
    private final ConcurrentLinkedQueue<BufferedOutput> queue;
    private final Instant timestamp;
    private final BoundedWindow window;

    BufferingOutput(
        ConcurrentLinkedQueue<BufferedOutput> queue, Instant timestamp, BoundedWindow window) {
      this.queue = queue;
      this.timestamp = timestamp;
      this.window = window;
    }

    @Override
    public void success(com.google.cloud.Timestamp value) {
      queue.add(new BufferedOutput(null, value, timestamp, window));
    }

    @Override
    public void error(
        TupleTag<FailsafeElement<String, String>> tag, FailsafeElement<String, String> element) {
      queue.add(new BufferedOutput(tag, element, timestamp, window));
    }
  }

  private String getCachedTxnTag(PipelineOptions options) {
    if (txnTag == null) {
      txnTag = getTxnTag(options);
    }
    return txnTag;
  }

  private synchronized void enterTransaction() {
    inFlightTransactions++;
    isInTransaction.set(true);
  }

  private synchronized void exitTransaction() {
    inFlightTransactions--;
    if (inFlightTransactions <= 0) {
      inFlightTransactions = 0;
      isInTransaction.set(false);
    }
  }

  /** An event waiting in a lane batch, with the output its outcome goes to. */
  static final class PendingEvent {
    final FailsafeElement<String, String> msg;
    final EventOutput out;

    PendingEvent(FailsafeElement<String, String> msg, EventOutput out) {
      this.msg = msg;
      this.out = out;
    }
  }

  /** Side inputs and settings captured on the runner thread for batches flushed at bundle end. */
  private static final class BatchContext {
    final Ddl ddl;
    final Ddl shadowTableDdl;
    final String txnTag;
    final MetricsContainer metricsContainer;

    BatchContext(Ddl ddl, Ddl shadowTableDdl, String txnTag, MetricsContainer metricsContainer) {
      this.ddl = ddl;
      this.shadowTableDdl = shadowTableDdl;
      this.txnTag = txnTag;
      this.metricsContainer = metricsContainer;
    }
  }

  /** An event that passed validation and can join a batched transaction. */
  private static final class PreparedEvent {
    final PendingEvent pending;
    final JsonNode changeEvent;
    final ChangeEventContext context;
    final ChangeEventSequence sequence;
    final List<String> shadowKeyColumns;
    final List<String> shadowSequenceColumns;
    final String migrationShardId;
    final boolean isRetryRecord;
    final Instant startTimestamp;

    PreparedEvent(
        PendingEvent pending,
        JsonNode changeEvent,
        ChangeEventContext context,
        ChangeEventSequence sequence,
        List<String> shadowKeyColumns,
        List<String> shadowSequenceColumns,
        String migrationShardId,
        boolean isRetryRecord,
        Instant startTimestamp) {
      this.pending = pending;
      this.changeEvent = changeEvent;
      this.context = context;
      this.sequence = sequence;
      this.shadowKeyColumns = shadowKeyColumns;
      this.shadowSequenceColumns = shadowSequenceColumns;
      this.migrationShardId = migrationShardId;
      this.isRetryRecord = isRetryRecord;
      this.startTimestamp = startTimestamp;
    }
  }

  /**
   * Applies events in arrival order, grouping consecutive batchable events into one transaction.
   * Anything that cannot be batched is applied on its own at its position, so per-row order holds.
   */
  void processEventBatch(List<PendingEvent> events, Ddl ddl, Ddl shadowTableDdl, String txnTag) {
    List<PreparedEvent> batch = new ArrayList<>();
    for (PendingEvent event : events) {
      PreparedEvent prepared = prepareForBatch(event, ddl, shadowTableDdl);
      if (prepared == null) {
        commitBatch(batch, ddl, shadowTableDdl, txnTag);
        processEvent(event.msg, ddl, shadowTableDdl, txnTag, event.out);
      } else {
        batch.add(prepared);
      }
    }
    commitBatch(batch, ddl, shadowTableDdl, txnTag);
  }

  /** Returns null when the event must go through the per-event path (which also reports errors). */
  private PreparedEvent prepareForBatch(PendingEvent event, Ddl ddl, Ddl shadowTableDdl) {
    if (usesSeparateShadowTableDb) {
      return null;
    }
    try {
      Instant startTimestamp = Instant.now();
      JsonNode changeEvent = mapper.readTree(event.msg.getPayload());
      JsonNode eventSourceTypeNode = changeEvent.get(DatastreamConstants.EVENT_SOURCE_TYPE_KEY);
      if (eventSourceTypeNode == null
          || !eventSourceTypeNode.asText().equalsIgnoreCase(sourceType)) {
        return null;
      }
      ChangeEventContext context =
          getSourceConnector()
              .createChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);
      if (context.getDataDmlStatement(ddl) != null) {
        return null;
      }
      List<String> sequenceColumns = getSourceConnector().getShadowSequenceColumns(context);
      if (sequenceColumns.isEmpty()) {
        return null;
      }
      Table shadowTable = shadowTableDdl.table(context.getShadowTable());
      if (shadowTable == null) {
        return null;
      }
      List<String> keyColumns = new ArrayList<>();
      for (IndexColumn keyColumn : shadowTable.primaryKeys()) {
        Column column = shadowTable.column(keyColumn.name());
        if (column == null || !BATCHABLE_KEY_TYPES.contains(column.type().getCode())) {
          return null;
        }
        keyColumns.add(keyColumn.name());
      }
      ChangeEventSequence sequence =
          getSourceConnector().createChangeEventSequenceFromChangeEventContext(context);
      String migrationShardId =
          Optional.ofNullable(changeEvent.get(SHARD_ID_COLUMN_NAME))
              .map(shardIdNode -> changeEvent.get(shardIdNode.asText()).asText())
              .orElse(null);
      JsonNode retryCount = changeEvent.get("_metadata_retry_count");
      if (retryCount != null) {
        eventRetries.update(retryCount.asLong());
      }
      return new PreparedEvent(
          event,
          changeEvent,
          context,
          sequence,
          keyColumns,
          sequenceColumns,
          migrationShardId,
          retryCount != null,
          startTimestamp);
    } catch (Exception e) {
      LOG.debug("Event not batchable, applying it on its own", e);
      return null;
    }
  }

  /* Key column types whose Spanner values map back to equal Key parts when read. */
  private static final Set<Type.Code> BATCHABLE_KEY_TYPES =
      EnumSet.of(
          Type.Code.INT64,
          Type.Code.STRING,
          Type.Code.BOOL,
          Type.Code.BYTES,
          Type.Code.DATE,
          Type.Code.TIMESTAMP,
          Type.Code.PG_INT8,
          Type.Code.PG_VARCHAR,
          Type.Code.PG_TEXT,
          Type.Code.PG_BOOL,
          Type.Code.PG_BYTEA,
          Type.Code.PG_DATE,
          Type.Code.PG_TIMESTAMPTZ);

  /* Read values are decoded with the client's type codes (PostgreSQL types map onto these). */

  static com.google.cloud.spanner.Key keyFromRow(Struct row, List<String> keyColumns) {
    com.google.cloud.spanner.Key.Builder key = com.google.cloud.spanner.Key.newBuilder();
    for (String column : keyColumns) {
      if (row.isNull(column)) {
        key.appendObject(null);
        continue;
      }
      switch (row.getColumnType(column).getCode()) {
        case INT64:
          key.append(row.getLong(column));
          break;
        case STRING:
          key.append(row.getString(column));
          break;
        case BOOL:
          key.append(row.getBoolean(column));
          break;
        case BYTES:
          key.append(row.getBytes(column));
          break;
        case DATE:
          key.append(row.getDate(column));
          break;
        case TIMESTAMP:
          key.append(row.getTimestamp(column));
          break;
        default:
          throw new IllegalStateException("Unsupported key column type for batching: " + column);
      }
    }
    return key.build();
  }

  private void commitBatch(List<PreparedEvent> batch, Ddl ddl, Ddl shadowTableDdl, String txnTag) {
    if (batch.isEmpty()) {
      return;
    }
    if (batch.size() == 1) {
      PendingEvent only = batch.get(0).pending;
      batch.clear();
      processEvent(only.msg, ddl, shadowTableDdl, txnTag, only.out);
      return;
    }
    boolean[] skipped = new boolean[batch.size()];
    enterTransaction();
    try {
      spannerAccessor
          .getDatabaseClient()
          .readWriteTransaction(
              Options.tag(txnTag),
              Options.excludeTxnFromChangeStreams(),
              Options.priority(spannerConfig.getRpcPriority().get()))
          .run(
              (TransactionRunner.TransactionCallable<Void>)
                  transaction -> {
                    transactionAttemptCount.incrementAndGet();
                    Arrays.fill(skipped, false);
                    // Latest applied sequence per (shadow table, key): from Spanner, then from
                    // earlier events of this batch.
                    Map<String, Map<com.google.cloud.spanner.Key, ChangeEventSequence>> latest =
                        new HashMap<>();
                    Map<String, List<PreparedEvent>> byTable = new LinkedHashMap<>();
                    for (PreparedEvent e : batch) {
                      byTable
                          .computeIfAbsent(e.context.getShadowTable(), t -> new ArrayList<>())
                          .add(e);
                    }
                    for (Map.Entry<String, List<PreparedEvent>> table : byTable.entrySet()) {
                      PreparedEvent first = table.getValue().get(0);
                      KeySet.Builder keys = KeySet.newBuilder();
                      Set<com.google.cloud.spanner.Key> expected = new HashSet<>();
                      for (PreparedEvent e : table.getValue()) {
                        if (expected.add(e.context.getPrimaryKey())) {
                          keys.addKey(e.context.getPrimaryKey());
                        }
                      }
                      List<String> columns = new ArrayList<>(first.shadowKeyColumns);
                      columns.addAll(first.shadowSequenceColumns);
                      Map<com.google.cloud.spanner.Key, ChangeEventSequence> sequences =
                          new HashMap<>();
                      try (ResultSet rs = transaction.read(table.getKey(), keys.build(), columns)) {
                        while (rs.next()) {
                          Struct row = rs.getCurrentRowAsStruct();
                          com.google.cloud.spanner.Key key =
                              keyFromRow(row, first.shadowKeyColumns);
                          if (!expected.contains(key)) {
                            // Never guess: fall back to the per-event path.
                            throw new IllegalStateException(
                                "Shadow row key " + key + " does not match any batched event");
                          }
                          sequences.put(
                              key,
                              getSourceConnector()
                                  .createChangeEventSequenceFromShadowRow(first.context, row));
                        }
                      }
                      latest.put(table.getKey(), sequences);
                    }
                    List<Mutation> mutations = new ArrayList<>();
                    for (int i = 0; i < batch.size(); i++) {
                      PreparedEvent e = batch.get(i);
                      Map<com.google.cloud.spanner.Key, ChangeEventSequence> sequences =
                          latest.get(e.context.getShadowTable());
                      ChangeEventSequence previous = sequences.get(e.context.getPrimaryKey());
                      if (previous != null && previous.compareTo(e.sequence) >= 0) {
                        skipped[i] = true;
                        continue;
                      }
                      e.context.getMutations().forEach(mutations::add);
                      sequences.put(e.context.getPrimaryKey(), e.sequence);
                    }
                    transaction.buffer(mutations);
                    return null;
                  });
    } catch (Exception e) {
      // Isolate the failing event(s): the per-event path classifies and routes each error.
      LOG.warn(
          "Batched transaction of {} events failed, applying them one by one", batch.size(), e);
      batchFallbacks.inc();
      List<PreparedEvent> retry = new ArrayList<>(batch);
      batch.clear();
      exitTransaction();
      for (PreparedEvent p : retry) {
        processEvent(p.pending.msg, ddl, shadowTableDdl, txnTag, p.pending.out);
      }
      return;
    }
    exitTransaction();
    batchedTransactions.inc();
    batchedEvents.update(batch.size());
    for (int i = 0; i < batch.size(); i++) {
      PreparedEvent e = batch.get(i);
      if (skipped[i]) {
        skippedEvents.inc();
      }
      e.pending.out.success(com.google.cloud.Timestamp.now());
      if (e.migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class,
                e.migrationShardId + " : " + SUCCESSFUL_EVENTS_COUNTER_NAME)
            .inc();
      }
      successfulEvents.inc();
      updateLatencyMetrics(e.changeEvent, e.startTimestamp);
      if (isRegularRunMode && e.isRetryRecord) {
        successfulEventRetries.inc();
      }
    }
    batch.clear();
  }

  void processEvent(
      FailsafeElement<String, String> msg,
      Ddl ddl,
      Ddl shadowTableDdl,
      String txnTag,
      EventOutput out) {
    Instant startTimestamp = Instant.now();
    String migrationShardId = null;
    String tableName = null;
    boolean isRetryRecord = false;
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      JsonNode tableNameNode = changeEvent.get(DatastreamConstants.EVENT_TABLE_NAME_KEY);
      tableName = tableNameNode != null ? tableNameNode.asText() : null;

      // Validate source type
      JsonNode eventSourceTypeNode = changeEvent.get(DatastreamConstants.EVENT_SOURCE_TYPE_KEY);
      String eventSourceType = eventSourceTypeNode != null ? eventSourceTypeNode.asText() : "";
      if (!eventSourceType.equalsIgnoreCase(sourceType)) {
        throw new InvalidChangeEventException(
            "Change event with invalid source. Actual("
                + eventSourceType
                + "), Expected("
                + sourceType
                + ")");
      }

      migrationShardId =
          Optional.ofNullable(changeEvent.get(SHARD_ID_COLUMN_NAME))
              .map(shardIdNode -> changeEvent.get(shardIdNode.asText()).asText())
              .orElse(null);
      JsonNode retryCount = changeEvent.get("_metadata_retry_count");

      if (retryCount != null) {
        eventRetries.update(retryCount.asLong());
        isRetryRecord = true;
      }
      ChangeEventContext changeEventContext =
          getSourceConnector()
              .createChangeEventContext(changeEvent, ddl, shadowTableDdl, shadowTablePrefix);

      // Sequence information for the current change event.
      ChangeEventSequence currentChangeEventSequence =
          getSourceConnector().createChangeEventSequenceFromChangeEventContext(changeEventContext);

      if (usesSeparateShadowTableDb) {
        processCrossDatabaseTransaction(
            txnTag, changeEventContext, currentChangeEventSequence, shadowTableDdl, ddl);
      } else {
        processSingleDatabaseTransaction(
            txnTag, changeEventContext, currentChangeEventSequence, shadowTableDdl, ddl);
      }
      com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
      out.success(timestamp);
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class,
                migrationShardId + " : " + SUCCESSFUL_EVENTS_COUNTER_NAME)
            .inc();
      }
      successfulEvents.inc();
      updateLatencyMetrics(changeEvent, startTimestamp);

      // increment the successful retry count if this was retry attempt
      if (isRegularRunMode && isRetryRecord) {
        successfulEventRetries.inc();
      }

    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion.
      LOG.warn(
          "Skipping change event for dropped table, tableName={}, changeEventMessage={}, error: {}",
          tableName,
          msg,
          e.getMessage());
      outputWithErrorTag(out, msg, e, DatastreamToSpannerConstants.SKIPPED_TABLE_EVENT_TAG);
      droppedTableExceptions.inc();
    } catch (InvalidChangeEventException e) {
      LOG.error("Invalid Change Exception", e);
      // Errors that result from invalid change events.
      outputWithErrorTag(out, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      invalidEvents.inc();
      if (migrationShardId != null) {
        Metrics.counter(SpannerTransactionWriterDoFn.class, migrationShardId + " : Invalid events")
            .inc();
      }
    } catch (ChangeEventConvertorException e) {
      LOG.error("Conversion Error", e);
      // Errors that result during Event conversions are not retryable.
      outputWithErrorTag(out, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class,
                migrationShardId + " : " + CONVERSION_ERRORS_COUNTER_NAME)
            .inc();
      }
      conversionErrors.inc();
    } catch (IllegalStateException ex) {
      /*
       * IllegalStateException can occur due to conditions like spanner pool being closed,
       * in which case if this event is requed to same or different node at a later point in time,
       * a retry might work.
       */
      outputWithErrorTag(out, msg, ex, DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG);
      // do not increment the retry error count if this was retry attempt
      if (!isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (SpannerException ex) {
      /*
       * There are many SpannerExceptions which can occur. Some of them are retryable and some of them are non-retryable.
       * Examples:
       * 1. Deadline exceeded errors from Cloud Spanner - Retryable error
       * 2. Failures due to foreign key/interleaved table constraints - Retryable error
       * 3. Unique index violation - Permanent error
       */
      SpannerMigrationException spannerMigrationException = SpannerExceptionParser.parse(ex);
      ErrorTag outputTag =
          DatastreamToSpannerExceptionClassifier.classify(spannerMigrationException);
      switch (outputTag) {
        case PERMANENT_ERROR:
          LOG.error(
              "A severe error occurred while processing the event.", spannerMigrationException);
          outputWithErrorTag(out, msg, ex, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
          break;
        case RETRYABLE_ERROR:
          LOG.warn(
              "A retryable error occurred while processing the event, the event will be retried again.",
              spannerMigrationException);
          outputWithErrorTag(out, msg, ex, DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG);
      }
      // do not increment the retry error count if this was retry attempt
      if (ErrorTag.RETRYABLE_ERROR.equals(outputTag) && !isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (Exception e) {
      LOG.error("Unhandled Exception", e);
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(out, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      failedEvents.inc();
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class, migrationShardId + " : Permanent errors")
            .inc();
      }
    }
  }

  private void processSingleDatabaseTransaction(
      String txnTag,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl ddl) {
    enterTransaction();
    try {
      runSingleDatabaseTransaction(
          txnTag, changeEventContext, currentChangeEventSequence, shadowDdl, ddl);
    } finally {
      exitTransaction();
    }
  }

  private void runSingleDatabaseTransaction(
      String txnTag,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl ddl) {
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag(txnTag),
            Options.excludeTxnFromChangeStreams(),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                transaction -> {
                  transactionAttemptCount.incrementAndGet();
                  // Sequence information for the last change event.
                  ChangeEventSequence previousChangeEventSequence =
                      getSourceConnector()
                          .createChangeEventSequenceFromShadowTable(
                              transaction, changeEventContext, shadowDdl, false);
                  /* There was a previous event recorded with a greater sequence information
                   * than current. Hence, skip the current event.
                   */
                  if (previousChangeEventSequence != null
                      && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                    skippedEvents.inc();
                    return null;
                  }
                  // Execute DML if applicable
                  Statement dataDml = changeEventContext.getDataDmlStatement(ddl);

                  if (dataDml != null) {
                    transaction.executeUpdate(dataDml);
                  }

                  // Apply shadow and data table mutations (only if they exist)
                  transaction.buffer(changeEventContext.getMutations());
                  return null;
                });
  }

  /**
   * This method does a cross database transaction across the shadow table db and the main db. It
   * performs the following steps:
   *
   * <ul>
   *   <li>Start shadow table transaction - tx1
   *   <li>Read the shadow table row with exclusive lock - tx1.read()
   *   <li>Start main table transaction - tx2
   *   <li>Read the main table row with exclusive lock - tx2.read()
   *   <li>Before committing tx2, read the shadow table row again using tx1 inside tx2.
   *   <li>Write to main table - tx2.write()
   *   <li>Commit main transaction - tx2.commit()
   *   <li>Update shadow table - tx1.write()
   *   <li>Commit shadow table transaction - tx1.commit()
   * </ul>
   *
   * <p>Step 5 acts as a lock validation ensuring that no other process is writing this row. If due
   * to some network partition/machine failure, the lock is lost during main table write, Spanner
   * will release the exclusive lock while the thread still continues its attempt to write. This
   * validation ensures that main table write also fails in such cases and the event goes for
   * retrial. This ensures no 2 processes are updating the same row together.
   */
  void processCrossDatabaseTransaction(
      String txnTag,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl dataTableDdl) {
    enterTransaction();
    try {
      runCrossDatabaseTransaction(
          txnTag, changeEventContext, currentChangeEventSequence, shadowDdl, dataTableDdl);
    } finally {
      exitTransaction();
    }
  }

  private void runCrossDatabaseTransaction(
      String txnTag,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl dataTableDdl) {
    shadowTableSpannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag(txnTag),
            Options.excludeTxnFromChangeStreams(),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .allowNestedTransaction()
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                shadowTxn -> {
                  transactionAttemptCount.incrementAndGet();

                  // Build lock query based on source type
                  ChangeEventSequence previousChangeEventSequence =
                      getSourceConnector()
                          .createChangeEventSequenceFromShadowTable(
                              shadowTxn,
                              changeEventContext,
                              shadowDdl,
                              /* useSqlStatments= */ true);

                  if (previousChangeEventSequence != null
                      && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                    skippedEvents.inc();
                    return null;
                  }

                  // Start main table transaction
                  spannerAccessor
                      .getDatabaseClient()
                      .readWriteTransaction(
                          Options.tag(txnTag),
                          Options.excludeTxnFromChangeStreams(),
                          Options.priority(spannerConfig.getRpcPriority().get()))
                      .run(
                          (TransactionRunner.TransactionCallable<Void>)
                              mainTxn -> {
                                // Read row from main table with lock scanned ranges to acquire
                                // exclusive lock on the main table row.
                                changeEventContext.readDataTableRowWithExclusiveLock(
                                    mainTxn, dataTableDdl);

                                // Validate the row still holds the exclusive lock. In case of
                                // network
                                // partitions, it could happen that Spanner releases the lock while
                                // this
                                // thread gets killed.
                                ChangeEventSequence validationSequence =
                                    getSourceConnector()
                                        .createChangeEventSequenceFromShadowTable(
                                            shadowTxn,
                                            changeEventContext,
                                            shadowDdl,
                                            /* useSqlStatments= */ true);

                                if (validationSequence != null
                                    && validationSequence.compareTo(previousChangeEventSequence)
                                        != 0) {
                                  // This code path should never execute since Spanner automatically
                                  // aborts transactions when locks are released.
                                  LOG.error(
                                      "Sequence mismatch: validation sequence {}, previous sequence {}",
                                      validationSequence,
                                      previousChangeEventSequence);
                                  throw new Exception(
                                      "Shadow table sequence changed during transaction");
                                }

                                // Execute Data DML if applicable
                                Statement dataDml =
                                    changeEventContext.getDataDmlStatement(dataTableDdl);
                                if (dataDml != null) {
                                  mainTxn.executeUpdate(dataDml);
                                }

                                // Write to main table
                                if (changeEventContext.getDataMutation() != null) {
                                  mainTxn.buffer(changeEventContext.getDataMutation());
                                }
                                return null;
                              });

                  // Update shadow table if main transaction succeeded
                  shadowTxn.buffer(changeEventContext.getShadowMutation());
                  return null;
                });
  }

  void updateLatencyMetrics(JsonNode changeEvent, Instant startTimestamp) {
    Instant endTimestamp = Instant.now();
    spannerWriterLatencyMs.update(new Duration(startTimestamp, endTimestamp).getMillis());
    long currentTimestampInSeconds = System.currentTimeMillis() / 1000L;
    long sourceTimestamp = changeEvent.get("_metadata_timestamp").asLong();
    long datastreamTimestamp = changeEvent.get("_metadata_read_timestamp").asLong();
    long dataflowTimestamp = changeEvent.get("_metadata_dataflow_timestamp").asLong();
    totalLatency.update(currentTimestampInSeconds - sourceTimestamp);
    systemLatency.update(currentTimestampInSeconds - datastreamTimestamp);
    dataflowLatency.update(currentTimestampInSeconds - dataflowTimestamp);
  }

  void outputWithErrorTag(
      EventOutput out,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    out.error(errorTag, output);
  }

  String getTxnTag(PipelineOptions options) {
    String jobId = "datastreamToSpanner";
    try {
      DataflowWorkerHarnessOptions harnessOptions = options.as(DataflowWorkerHarnessOptions.class);
      jobId = harnessOptions.getJobId();
    } catch (Exception e) {
      LOG.warn(
          "Unable to find Dataflow job id. Spanner transaction tags will not contain the dataflow"
              + " job id.",
          e);
    }
    // Spanner transaction tags have a limit of 50 characters. Dataflow job id is 40 chars in
    // length.
    String txnTag = "txBy=" + jobId;
    if (txnTag.length() > MAX_TXN_TAG_LENGTH) {
      txnTag = txnTag.substring(0, MAX_TXN_TAG_LENGTH);
    }
    return txnTag;
  }

  private IDsToSpSourceConnector getSourceConnector() {
    if (sourceConnector == null) {
      sourceConnector = DatastreamToSpannerSourceConnectorRegistry.getSourceConnector(sourceType);
    }
    return sourceConnector;
  }

  void setSourceConnector(IDsToSpSourceConnector sourceConnector) {
    this.sourceConnector = sourceConnector;
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public void setSpannerAccessor(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
  }

  public void setShadowTableSpannerAccessor(SpannerAccessor shadowTableSpannerAccessor) {
    this.shadowTableSpannerAccessor = shadowTableSpannerAccessor;
  }

  public void setTransactionAttemptCount(AtomicLong transactionAttemptCount) {
    this.transactionAttemptCount = transactionAttemptCount;
  }

  public void setIsInTransaction(AtomicBoolean isInTransaction) {
    this.isInTransaction = isInTransaction;
  }
}
