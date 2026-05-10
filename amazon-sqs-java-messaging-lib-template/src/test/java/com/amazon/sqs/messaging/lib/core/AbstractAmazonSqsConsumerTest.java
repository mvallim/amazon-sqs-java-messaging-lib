/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.sqs.messaging.lib.core;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mock.Strictness;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.core.RequestEntryInternalFactory.RequestEntryInternal;
import com.amazon.sqs.messaging.lib.exception.MaximumAllowedMessageException;
import com.amazon.sqs.messaging.lib.helpers.TryConsumer;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

// @formatter:off
@ExtendWith(MockitoExtension.class)
class AbstractAmazonSqsConsumerTest {

  private static final String QUEUE_URL = "http://localhost/000000000000/test-queue";
  private static final long LINGER_MS = 50L;
  private static final int MAX_BATCH_SIZE = 10;

  @Mock(strictness = Strictness.LENIENT)
  private Object amazonSnsClient;

  @Mock(strictness = Strictness.LENIENT)
  private QueueProperty queueProperty;

  @Mock(strictness = Strictness.LENIENT)
  private ExecutorService executorService;

  @Mock(strictness = Strictness.LENIENT)
  private ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> listenableFutureImpl;

  private ObjectMapper objectMapper;

  private ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests;

  private BlockingQueue<RequestEntry<String>> queueRequests;

  private UnaryOperator<Object> publishDecorator;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    pendingRequests = new ConcurrentHashMap<>();
    queueRequests = new RingBufferBlockingQueue<>();
    publishDecorator = UnaryOperator.identity();

    when(queueProperty.getQueueUrl()).thenReturn(QUEUE_URL);
    when(queueProperty.getLinger()).thenReturn(LINGER_MS);
    when(queueProperty.getMaxBatchSize()).thenReturn(MAX_BATCH_SIZE);
    when(queueProperty.isFifo()).thenReturn(false);
  }

  @Test
  void testConstructorInitializesPendingRequests() {
    assertThat(pendingRequests, is(notNullValue()));
    assertThat(pendingRequests.isEmpty(), is(true));
  }

  @Test
  void testConstructorInitializesqueueRequests() {
    assertThat(queueRequests, is(notNullValue()));
    assertThat(queueRequests.isEmpty(), is(true));
  }

  @Test
  void testAwaitReturnCompletableFutureWhenQueuesAreEmpty() throws Exception {
    context(consumer -> {
      final CompletableFuture<Void> future = consumer.await();
      assertThat(future, is(notNullValue()));
      future.get(2, TimeUnit.SECONDS);
      assertThat(future.isDone(), is(true));
    });
  }

  @Test
  void testAwaitReturnNonNullFuture() throws Exception {
    context(consumer -> {
      final CompletableFuture<Void> future = consumer.await();
      assertThat(future, is(notNullValue()));
    });
  }

  @Test
  void testAwaitCompletesWhenPendingRequestsAndqueueRequestsAreEmpty() throws Exception {
    context(consumer -> {
      final CompletableFuture<Void> future = consumer.await();
      future.get(2, TimeUnit.SECONDS);

      assertThat(future.isDone(), is(true));
      assertThat(future.isCompletedExceptionally(), is(false));
    });
  }

  @Test
  void testAwaitEventuallyCompletesAfterPendingRequestsAreCleared() throws Exception {
    pendingRequests.put("key-1", listenableFutureImpl);

    context(consumer -> {
      final CompletableFuture<Void> future = consumer.await();

      assertThat(future.isDone(), is(false));

      pendingRequests.clear();

      future.get(3, TimeUnit.SECONDS);
      assertThat(future.isDone(), is(true));
    });
  }

  @Test
  void testShutdownDoesNotThrowException() throws Exception {
    context(consumer -> {
      try {
        consumer.shutdown();
      } catch (final Exception ex) {
        assertThat("shutdown should not throw an exception", false, is(true));
      }
    });
  }

  @Test
  void testShutdownCanBeCalledMultipleTimes() throws Exception {
    context(consumer -> {
      try {
        consumer.shutdown();
        consumer.shutdown();
      } catch (final Exception ex) {
        assertThat("multiple shutdown calls should not throw an exception", false, is(true));
      }
    });
  }

  @Test
  void testRunWithFifoqueuePublishesSynchronously() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(executorService.submit(any(Runnable.class))).thenReturn(null);

    context(consumer -> {
      final RequestEntry<String> entry = buildRequestEntry("fifo-message");
      queueRequests.put(entry);

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(1));
      assertThat(consumer.getHandleResponseCallCount(), greaterThanOrEqualTo(1));
    });
  }

  @Test
  void testRunWithNonFifoqueuePublishesAsynchronously() throws Exception {
    when(queueProperty.isFifo()).thenReturn(false);
    doAnswer(inv -> {
      ((Runnable) inv.getArgument(0)).run();
      return CompletableFuture.completedFuture(null);
    }).when(executorService).execute(any(Runnable.class));

    context(consumer -> {
      final RequestEntry<String> entry = buildRequestEntry("non-fifo-message");
      queueRequests.put(entry);

      Thread.sleep(LINGER_MS * 3);

      verify(executorService, atLeastOnce()).execute(any(Runnable.class));
    });
  }

  @Test
  void testRunHandlesPublishExceptionWithoutCrashing() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      consumer.setThrowOnPublish(true);

      final RequestEntry<String> entry = buildRequestEntry("error-message");
      queueRequests.put(entry);

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getHandleErrorCallCount(), greaterThanOrEqualTo(1));
      assertThat(consumer.getLastError(), is(notNullValue()));
    });
  }

  @Test
  void testRunRecordsCorrectExceptionOnPublishFailure() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      consumer.setThrowOnPublish(true);

      queueRequests.put(buildRequestEntry("fail-message"));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getLastError(), instanceOf(RuntimeException.class));
      assertThat(consumer.getLastError().getMessage(), containsString("publish failed"));
    });
  }

  @Test
  void testRunDoesNotPublishWhenQueueIsEmpty() throws Exception {
    Thread.sleep(LINGER_MS * 3);

    context(consumer -> {
      assertThat(consumer.getPublishCallCount(), is(0));
      assertThat(consumer.getHandleErrorCallCount(), is(0));
    });
  }

  @Test
  void testRunPublishesMultipleEntriesInSingleBatch() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      for (int i = 0; i < 5; i++) {
        queueRequests.put(buildRequestEntry("message-" + i));
      }

      Thread.sleep(LINGER_MS * 4);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(1));
    });
  }

  @Test
  void testRunRespectMaxBatchSizeByPublishingInMultipleBatches() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(queueProperty.getMaxBatchSize()).thenReturn(2);

    context(consumer -> {
      for (int i = 0; i < 6; i++) {
        queueRequests.put(buildRequestEntry("msg-" + i));
      }

      Thread.sleep(LINGER_MS * 6);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(2));
    });
  }

  @Test
  void testPendingRequestsIsEmptyOnConstruction() {
    assertThat(pendingRequests.isEmpty(), is(true));
  }

  @Test
  void testPendingRequestsCanHoldMultipleEntries() {
    pendingRequests.put("id-1", listenableFutureImpl);
    pendingRequests.put("id-2", listenableFutureImpl);

    assertThat(pendingRequests.size(), is(2));
    assertThat(pendingRequests, hasKey("id-1"));
    assertThat(pendingRequests, hasKey("id-2"));
  }

  @Test
  void testPendingRequestsCanBeRemovedAfterProcessing() {
    pendingRequests.put("id-1", listenableFutureImpl);
    pendingRequests.remove("id-1");

    assertThat(pendingRequests.isEmpty(), is(true));
  }

  @Test
  void testqueueRequestsIsEmptyOnConstruction() {
    assertThat(queueRequests.isEmpty(), is(true));
  }

  @Test
  void testqueueRequestsAcceptsRequestEntries() throws InterruptedException {
    queueRequests.put(buildRequestEntry("payload-1"));
    queueRequests.put(buildRequestEntry("payload-2"));

    assertThat(queueRequests.size(), is(2));
  }

  @Test
  void testqueueRequestsPollRemovesEntry() throws InterruptedException {
    queueRequests.put(buildRequestEntry("payload"));

    final RequestEntry<String> polled = queueRequests.take();

    assertThat(polled, is(notNullValue()));
    assertThat(queueRequests.isEmpty(), is(true));
  }

  @Test
  void testPublishDecoratorIsAppliedBeforePublish() throws Exception {
    final Object decoratedObject = new Object();
    final UnaryOperator<Object> trackingDecorator = req -> decoratedObject;

    when(queueProperty.isFifo()).thenReturn(true);

    context(trackingDecorator, consumer -> {
      queueRequests.put(buildRequestEntry("decorated-message"));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(1));
    });
  }

  @Test
  void testPublishDecoratorIdentityDoesNotAlterRequest() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      queueRequests.put(buildRequestEntry("identity-message"));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(1));
      assertThat(consumer.getHandleErrorCallCount(), is(0));
    });
  }

  @Test
  void testCanAddPayloadAllowsEntryWellBelowSizeThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      queueRequests.put(buildRequestEntry("small-payload"));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getTotalPublishedEntries(), is(1));
      assertThat(consumer.getHandleErrorCallCount(), is(0));
    });
  }

  @Test
  void testCanAddPayloadAllowsEntryExactlyAtSizeThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      final String payloadAtThreshold = buildPayloadOfBytes(TestableAmazonSqsConsumer.batchSizeBytesThreshold());
      queueRequests.put(buildRequestEntry(payloadAtThreshold));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getTotalPublishedEntries(), is(1));
      assertThat(consumer.getHandleErrorCallCount(), is(0));
    });
  }

  @Test
  void testCanAddPayloadRejectsEntryExceedingSizeThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      final String oversizedPayload = buildPayloadOfBytes(TestableAmazonSqsConsumer.batchSizeBytesThreshold() + 1);
      queueRequests.put(buildRequestEntry(oversizedPayload));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getTotalPublishedEntries(), is(1));
      assertThat(consumer.getHandleErrorCallCount(), greaterThanOrEqualTo(1));
      assertThat(consumer.getLastError(), instanceOf(MaximumAllowedMessageException.class));
      assertThat(consumer.getLastError().getMessage(), containsString("1024KB"));
    });
  }

  @Test
  void testCanAddPayloadStopsAccumulatingWhenBatchExceedsThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(queueProperty.getMaxBatchSize()).thenReturn(10);

    context(consumer -> {
      final int halfThreshold = TestableAmazonSqsConsumer.batchSizeBytesThreshold() / 2;
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(halfThreshold)));
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(halfThreshold)));
      queueRequests.put(buildRequestEntry("small-overflow"));

      Thread.sleep(LINGER_MS * 6);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(2));
      assertThat(consumer.getTotalPublishedEntries(), is(3));
    });
  }

  @Test
  void testCanAddPayloadPublishesFirstEntryAloneWhenItFillsThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(queueProperty.getMaxBatchSize()).thenReturn(10);

    context(consumer -> {
      final int fullThreshold = TestableAmazonSqsConsumer.batchSizeBytesThreshold();
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(fullThreshold)));
      queueRequests.put(buildRequestEntry("second-entry"));

      Thread.sleep(LINGER_MS * 6);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(2));
      assertThat(consumer.getPublishedBatchSizes().get(0), is(1));
    });
  }

  @Test
  void testCanAddPayloadAllowsMultipleSmallEntriesUpToThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(queueProperty.getMaxBatchSize()).thenReturn(100);

    context(consumer -> {
      for (int i = 0; i < 10; i++) {
        queueRequests.put(buildRequestEntry("entry-" + i));
      }

      Thread.sleep(LINGER_MS * 4);

      assertThat(consumer.getTotalPublishedEntries(), is(10));
      assertThat(consumer.getHandleErrorCallCount(), is(0));
    });
  }

  @Test
  void testCanAddPayloadSplitsBatchWhenCumulativeSizeExceedsThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);
    when(queueProperty.getMaxBatchSize()).thenReturn(10);

    context(consumer -> {
      final int chunkSize = (TestableAmazonSqsConsumer.batchSizeBytesThreshold() / 3) + 1;
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(chunkSize)));
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(chunkSize)));
      queueRequests.put(buildRequestEntry(buildPayloadOfBytes(chunkSize)));

      Thread.sleep(LINGER_MS * 8);

      assertThat(consumer.getPublishCallCount(), greaterThanOrEqualTo(2));
      assertThat(consumer.getTotalPublishedEntries(), is(3));
    });
  }

  @Test
  void testCanAddPayloadDoesNotPublishEmptyBatchWhenAllEntriesExceedThreshold() throws Exception {
    when(queueProperty.isFifo()).thenReturn(true);

    context(consumer -> {
      final String oversizedPayload = buildPayloadOfBytes(TestableAmazonSqsConsumer.batchSizeBytesThreshold() + 100);
      queueRequests.put(buildRequestEntry(oversizedPayload));

      Thread.sleep(LINGER_MS * 3);

      assertThat(consumer.getTotalPublishedEntries(), is(1));
      assertThat(consumer.getHandleErrorCallCount(), greaterThanOrEqualTo(1));
    });
  }

  private RequestEntry<String> buildRequestEntry(final String value) {
    return RequestEntry.<String>builder().withValue(value).build();
  }

  private String buildPayloadOfBytes(final int targetBytes) {
    return StringUtils.repeat('x', Math.max(0, targetBytes));
  }

  private void context(final TryConsumer<TestableAmazonSqsConsumer> consumer) throws Exception {
    try (final TestableAmazonSqsConsumer snsConsumer = new TestableAmazonSqsConsumer(amazonSnsClient, queueProperty, objectMapper, pendingRequests, queueRequests, executorService, publishDecorator)) {
      consumer.accept(snsConsumer);
    }
  }

  private void context(final UnaryOperator<Object> trackingDecorator, final TryConsumer<TestableAmazonSqsConsumer> consumer) throws Exception {
    try (final TestableAmazonSqsConsumer snsConsumer = new TestableAmazonSqsConsumer(amazonSnsClient, queueProperty, objectMapper, pendingRequests, queueRequests, executorService, trackingDecorator)) {
      consumer.accept(snsConsumer);
    }
  }

  static class TestableAmazonSqsConsumer extends AbstractAmazonSqsConsumer<Object, Object, Object, String> implements AutoCloseable {

    private static final int BATCH_SIZE_BYTES_THRESHOLD = 1024 * 1024;

    private final AtomicInteger publishCallCount = new AtomicInteger(0);
    private final AtomicInteger handleErrorCallCount = new AtomicInteger(0);
    private final AtomicInteger handleResponseCallCount = new AtomicInteger(0);
    private Throwable lastError;
    private boolean throwOnPublish = false;
    private final RuntimeException publishException = new RuntimeException("publish failed");
    private final List<Integer> publishedBatchSizes = new LinkedList<>();

    TestableAmazonSqsConsumer(
        final Object amazonSnsClient,
        final QueueProperty queueProperty,
        final ObjectMapper objectMapper,
        final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests,
        final BlockingQueue<RequestEntry<String>> queueRequests,
        final ExecutorService executorService,
        final UnaryOperator<Object> publishDecorator) {
      super(amazonSnsClient, queueProperty, objectMapper, pendingRequests, queueRequests, executorService, publishDecorator);
    }

    @Override
    protected Object publish(final Object publishBatchRequest) {
      publishCallCount.incrementAndGet();
      if (throwOnPublish) {
        throw publishException;
      }
      return new Object();
    }

    @Override
    protected void handleError(final Object publishBatchRequest, final Throwable throwable) {
      handleErrorCallCount.incrementAndGet();
      lastError = throwable;
    }

    @Override
    protected void handleResponse(final Object publishBatchResult) {
      handleResponseCallCount.incrementAndGet();
    }

    @Override
    protected BiFunction<String, List<RequestEntryInternal>, Object> supplierPublishRequest() {
      return (queueUrl, entries) -> {
        publishedBatchSizes.add(entries.size());
        return new Object();
      };
    }

    int getPublishCallCount() {
      return publishCallCount.get();
    }

    int getHandleErrorCallCount() {
      return handleErrorCallCount.get();
    }

    int getHandleResponseCallCount() {
      return handleResponseCallCount.get();
    }

    Throwable getLastError() {
      return lastError;
    }

    void setThrowOnPublish(final boolean throwOnPublish) {
      this.throwOnPublish = throwOnPublish;
    }

    List<Integer> getPublishedBatchSizes() {
      return publishedBatchSizes;
    }

    int getTotalPublishedEntries() {
      return publishedBatchSizes.stream().mapToInt(Integer::intValue).sum();
    }

    static int batchSizeBytesThreshold() {
      return BATCH_SIZE_BYTES_THRESHOLD;
    }

    @Override
    public void close() throws Exception {
      shutdown();
    }

  }

}
//@formatter:on