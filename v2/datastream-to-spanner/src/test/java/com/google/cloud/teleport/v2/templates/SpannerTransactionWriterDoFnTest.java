/*
 * Copyright (C) 2024 Google LLC
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

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for SpannerTransactionWriterDoFn class. */
public class SpannerTransactionWriterDoFnTest {

  @Test
  public void testGetTxnTag() {
    String[] args = new String[] {"--jobId=123"};
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, null, null, "", "mysql", true);
    String result = spannerTransactionWriterDoFn.getTxnTag(options);
    assertEquals(result, "txBy=123");
  }

  Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("version")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  @Test
  public void testProcessElement() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(failsafeElement);
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any())).thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(1)).buffer(argument.capture());
    Iterable<Mutation> capturedMutations = argument.getValue();
    Iterator<Mutation> mutationIterator = capturedMutations.iterator();
    Mutation actualDataMutation = null;
    Mutation actualShadowTableMutation = null;

    if (mutationIterator.hasNext()) {
      // Get the first mutation
      actualDataMutation = mutationIterator.next();

      if (mutationIterator.hasNext()) {
        // Get the second mutation
        actualShadowTableMutation = mutationIterator.next();
      }
    }

    Mutation.WriteBuilder dataBuilder = Mutation.newInsertOrUpdateBuilder("Users");
    dataBuilder.set("first_name").to("Johnny");
    dataBuilder.set("last_name").to("Depp");
    dataBuilder.set("age").to(13);
    Mutation expectedDataMutation = dataBuilder.build();
    assertEquals(actualDataMutation, expectedDataMutation);

    Mutation.WriteBuilder shadowBuilder = Mutation.newInsertOrUpdateBuilder("shadow_Users");
    shadowBuilder.set("first_name").to("Johnny");
    shadowBuilder.set("last_name").to("Depp");
    shadowBuilder.set("timestamp").to(12345);
    shadowBuilder.set("log_file").to("");
    shadowBuilder.set("log_position").to(-1);
    Mutation expectedShadowMutation = shadowBuilder.build();
    assertEquals(actualShadowTableMutation, expectedShadowMutation);

    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementWithInvalidChangeEvent() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);

    String instanceId = "test-instance";
    String databaseId = "test-database";
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of(instanceId));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of(databaseId));

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "random");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users1");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 123);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(failsafeElement);
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(schema.isEmpty()).thenReturn(true);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertEquals(
        "Change event with invalid source. Actual(random), Expected(mysql)",
        argument.getValue().getErrorMessage());
  }

  @Test
  public void testProcessElementWithRetryableSpannerException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(failsafeElement);
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ABORTED, "Transaction Aborted");
            });
    when(databaseClientMock.readWriteTransaction(any(), any())).thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(0)).buffer(anyList());

    verify(processContextMock, times(1))
        .output(eq(RETRYABLE_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementWithNonRetryableSpannerException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(failsafeElement);
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION, "title must not be NULL in table Books");
            });
    when(databaseClientMock.readWriteTransaction(any(), any())).thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(0)).buffer(anyList());

    verify(processContextMock, times(1))
        .output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }
}
