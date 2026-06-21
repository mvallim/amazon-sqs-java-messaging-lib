/*
 * Copyright 2022 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.messaging.lib.concurrent.ThreadFactoryProvider;
import com.amazon.sqs.messaging.lib.core.RequestEntryInternalFactory.RequestEntryInternal;
import com.amazon.sqs.messaging.lib.exception.MaximumAllowedMessageException;
import com.amazon.sqs.messaging.lib.model.PublishRequestBuilder;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

// @formatter:off
/**
 * Abstract base class for Amazon SQS message consumers. Periodically drains a
 * {@link BlockingQueue} of {@link RequestEntry} items, batches them, and publishes
 * them to SQS. Subclasses implement the actual publish and response handling logic.
 *
 * @param <C> the Amazon SQS client type
 * @param <R> the publish batch request type
 * @param <O> the publish batch result type
 * @param <E> the request entry payload type
 */
abstract class AbstractAmazonSqsConsumer<C, R, O, E> implements Runnable, AmazonSqsConsumer<R, O> {

  /**
   * Kilobyte constant used for size calculations.
   */
  private static final Integer KB = 1024;

  /**
   * Maximum batch size threshold of 1024 KB imposed by Amazon SQS.
   */
  private static final Integer BATCH_SIZE_BYTES_THRESHOLD = 1024 * KB;

  /** Class logger. */
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAmazonSqsConsumer.class);

  /** Single-thread scheduler that periodically triggers batch draining. */
  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(ThreadFactoryProvider.getThreadFactory());

  /** The Amazon SQS client used for publishing batches. */
  protected final C amazonSqsClient;

  /** The topic configuration properties. */
  private final QueueProperty queueProperty;

  /** Factory for creating internal request entry representations. */
  private final RequestEntryInternalFactory requestEntryInternalFactory;

  /** Shared map of pending requests keyed by request ID for async completion. */
  protected final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests;

  /** The blocking queue that buffers incoming topic requests. */
  private final BlockingQueue<RequestEntry<E>> queueRequests;

  /** Optional decorator applied to the publish batch request before sending. */
  private final UnaryOperator<R> publishDecorator;

  /** Executor service for asynchronous (non-FIFO) publishing. */
  private final ExecutorService executorService;

  /**
   * Creates a new abstract consumer.
   *
   * @param amazonSqsClient  the Amazon SQS client
   * @param queueProperty    the queue configuration
   * @param objectMapper     the Jackson ObjectMapper for payload serialization
   * @param pendingRequests  the shared map of pending requests keyed by request ID
   * @param queueRequests    the shared blocking queue for queue requests
   * @param executorService  the executor service for async publishing
   * @param publishDecorator a decorator for the publish batch request
   */
  protected AbstractAmazonSqsConsumer(
      final C amazonSqsClient,
      final QueueProperty queueProperty,
      final ObjectMapper objectMapper,
      final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests,
      final BlockingQueue<RequestEntry<E>> queueRequests,
      final ExecutorService executorService,
      final UnaryOperator<R> publishDecorator) {

    this.amazonSqsClient = amazonSqsClient;
    this.queueProperty = queueProperty;
    requestEntryInternalFactory = new RequestEntryInternalFactory(objectMapper);
    this.pendingRequests = pendingRequests;
    this.queueRequests = queueRequests;
    this.publishDecorator = publishDecorator;
    this.executorService = executorService;

    scheduledExecutorService.scheduleAtFixedRate(this, 0, queueProperty.getLinger(), TimeUnit.MILLISECONDS);
  }

  /**
   * Provides a factory function that creates a batch publish request from a queue URL
   * and a list of internal request entries.
   *
   * @return a bi-function that creates batch publish requests
   */
  protected abstract BiFunction<String, List<RequestEntryInternal>, R> supplierPublishRequest();

  /**
   * Executes the publish operation and handles the response or error.
   *
   * @param publishBatchRequest the batch publish request to send
   */
  private void doPublish(final R publishBatchRequest) {
    try {
      handleResponse(publish(publishDecorator.apply(publishBatchRequest)));
    } catch (final Exception ex) {
      handleError(publishBatchRequest, ex);
    }
  }

  /**
   * Publishes a batch of messages, running synchronously for FIFO queues or
   * asynchronously for standard queues.
   *
   * @param publishBatchRequest the batch publish request to send
   */
  private void publishBatch(final R publishBatchRequest) {
    try {
      final Runnable runnable = () -> doPublish(publishBatchRequest);

      if (queueProperty.isFifo()) {
        runnable.run();
      } else {
        CompletableFuture.runAsync(runnable, executorService);
      }
    } catch (final Exception ex) {
      handleError(publishBatchRequest, ex);
    }
  }

  /**
   * Periodically drains the request queue and publishes batches.
   */
  @Override
  @SneakyThrows
  public void run() {
    try {
      while (requestsWaitedFor(queueRequests, queueProperty.getLinger()) || maxBatchSizeReached(queueRequests)) {
        createBatch(queueRequests).ifPresent(this::publishBatch);
      }
    } catch (final Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
    }
  }

  /**
   * Shuts down the consumer, waiting for all pending requests to complete
   * before terminating the scheduled and executor services.
   */
  @Override
  @SneakyThrows
  public void shutdown() {
    await().thenRun(() -> {
      try {
        LOGGER.warn("Shutdown consumer {}", getClass().getSimpleName());

        scheduledExecutorService.shutdown();
        if (!scheduledExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
          LOGGER.warn("Scheduled executor service did not terminate in the specified time.");
          final List<Runnable> droppedTasks = scheduledExecutorService.shutdownNow();
          LOGGER.warn("Scheduled executor service was abruptly shut down. {} tasks will not be executed.", droppedTasks.size());
        }

        executorService.shutdown();
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          LOGGER.warn("Executor service did not terminate in the specified time.");
          final List<Runnable> droppedTasks = executorService.shutdownNow();
          LOGGER.warn("Executor service was abruptly shut down. {} tasks will not be executed.", droppedTasks.size());
        }
      } catch (final InterruptedException ex) {
        LOGGER.error(ex.getMessage(), ex);
        Thread.currentThread().interrupt();
      }
    }).join();
  }

  /**
   * Checks if the oldest pending request has waited longer than the batching window.
   *
   * @param requests           the blocking queue of requests
   * @param batchingWindowInMs the batching window in milliseconds
   * @return true if the oldest request has exceeded the batching window
   */
  private boolean requestsWaitedFor(final BlockingQueue<RequestEntry<E>> requests, final long batchingWindowInMs) {
    return Optional.ofNullable(requests.peek()).map(oldestPendingRequest -> {
      final long oldestEntryWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - oldestPendingRequest.getCreateTime());
      return oldestEntryWaitTime > batchingWindowInMs;
    }).orElse(false);
  }

  /**
   * Checks if the number of queued requests exceeds the maximum batch size.
   *
   * @param requests the blocking queue of requests
   * @return true if the queue size exceeds the maximum batch size
   */
  private boolean maxBatchSizeReached(final BlockingQueue<RequestEntry<E>> requests) {
    return requests.size() > queueProperty.getMaxBatchSize();
  }

  /**
   * Determines whether a request can be added to the current batch based on size and count limits.
   *
   * @param batchSizeBytes     the current batch size in bytes
   * @param requestEntriesSize the current number of entries in the batch
   * @param request            the request to evaluate
   * @return true if the request can be added to the batch
   */
  private boolean canAddToBatch(final int batchSizeBytes, final int requestEntriesSize, final RequestEntry<E> request) {
    return (batchSizeBytes < BATCH_SIZE_BYTES_THRESHOLD)
      && (requestEntriesSize < queueProperty.getMaxBatchSize())
      && Objects.nonNull(request);
  }

  /**
   * Checks if the batch size is within the allowed payload threshold.
   *
   * @param batchSizeBytes the current batch size in bytes
   * @return true if the batch size is within the threshold
   */
  private boolean canAddPayload(final int batchSizeBytes) {
    return batchSizeBytes <= BATCH_SIZE_BYTES_THRESHOLD;
  }

  /**
   * Drains requests from the queue and groups them into a batch publish request,
   * respecting size limits and handling oversized messages.
   *
   * @param requests the blocking queue of requests
   * @return an optional batch publish request, empty if no requests could be batched
   */
  @SneakyThrows
  private Optional<R> createBatch(final BlockingQueue<RequestEntry<E>> requests) {
    final AtomicInteger batchSizeBytes = new AtomicInteger(0);
    final List<RequestEntryInternal> requestEntries = new LinkedList<>();

    while (canAddToBatch(batchSizeBytes.get(), requestEntries.size(), requests.peek())) {
      final RequestEntry<E> request = requests.peek();

      final byte[] payload = requestEntryInternalFactory.convertPayload(request);

      final Integer messageBodySize = payload.length;
      final Integer messageAttributesSize = requestEntryInternalFactory.messageAttributesSize(request);

      final Integer messageSize = messageBodySize + messageAttributesSize;

      if (messageSize > BATCH_SIZE_BYTES_THRESHOLD) {
        final R publishBatchRequest = PublishRequestBuilder.<R, RequestEntryInternal>builder()
          .supplier(supplierPublishRequest())
          .entries(Collections.singletonList(requestEntryInternalFactory.create(request, payload)))
          .queueUrl(queueProperty.getQueueUrl())
          .build();

        final String stringPayload = new String(payload, StandardCharsets.UTF_8);

        final String message = String.format("The maximum allowed message size exceeding 1024KB (1,048,576 bytes). Payload: %s, Headers: %s", stringPayload, request.getMessageHeaders());

        handleError(publishBatchRequest, new MaximumAllowedMessageException(message, requests.take()));

        // This entry was rejected and already removed from the queue above; its size
        // must NOT be folded into batchSizeBytes, or it would wrongly cut the batch
        // short even when smaller, valid entries are still waiting right behind it.
        continue;
      }

      if (canAddPayload(batchSizeBytes.addAndGet(messageSize))) {
        requestEntries.add(requestEntryInternalFactory.create(requests.take(), payload));
      }
    }

    if (requestEntries.isEmpty()) {
      return Optional.empty();
    }

    LOGGER.debug("{}", requestEntries);

    return Optional.of(PublishRequestBuilder.<R, RequestEntryInternal>builder()
      .supplier(supplierPublishRequest())
      .entries(requestEntries)
      .queueUrl(queueProperty.getQueueUrl())
      .build());
  }

  /**
   * Returns a {@link CompletableFuture} that completes when all pending and queued
   * requests have been processed.
   *
   * @return a future that completes when all requests are processed
   */
  @Override
  @SneakyThrows
  public CompletableFuture<Void> await() {
    return CompletableFuture.runAsync(() -> {
      while (MapUtils.isNotEmpty(this.pendingRequests) || CollectionUtils.isNotEmpty(this.queueRequests)) {
        LockSupport.parkNanos(Duration.ofMillis(queueProperty.getLinger()).toNanos());
      }
    });
  }

}
// @formatter:on
