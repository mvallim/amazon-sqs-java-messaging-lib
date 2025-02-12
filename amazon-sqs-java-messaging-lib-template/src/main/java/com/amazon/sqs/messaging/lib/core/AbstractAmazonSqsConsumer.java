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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.messaging.lib.core.RequestEntryInternalFactory.RequestEntryInternal;
import com.amazon.sqs.messaging.lib.model.PublishRequestBuilder;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

// @formatter:off
abstract class AbstractAmazonSqsConsumer<C, R, O, E> implements Runnable {

  private static final Integer KB = 1024;

  private static final Integer BATCH_SIZE_BYTES_THRESHOLD = 256 * KB;

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAmazonSqsConsumer.class);

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  protected final C amazonSqsClient;

  private final QueueProperty queueProperty;

  private final RequestEntryInternalFactory requestEntryInternalFactory;

  protected final ConcurrentMap<String, ListenableFutureRegistry> pendingRequests;

  private final BlockingQueue<RequestEntry<E>> queueRequests;

  private final UnaryOperator<R> publishDecorator;

  private final ExecutorService executorService;

  protected AbstractAmazonSqsConsumer(
      final C amazonSqsClient,
      final QueueProperty queueProperty,
      final ObjectMapper objectMapper,
      final ConcurrentMap<String, ListenableFutureRegistry> pendingRequests,
      final BlockingQueue<RequestEntry<E>> queueRequests,
      final ExecutorService executorService,
      final UnaryOperator<R> publishDecorator) {

    this.amazonSqsClient = amazonSqsClient;
    this.queueProperty = queueProperty;
    this.requestEntryInternalFactory = new RequestEntryInternalFactory(objectMapper);
    this.pendingRequests = pendingRequests;
    this.queueRequests = queueRequests;
    this.publishDecorator = publishDecorator;
    this.executorService = executorService;

    scheduledExecutorService.scheduleAtFixedRate(this, 0, queueProperty.getLinger(), TimeUnit.MILLISECONDS);
  }

  protected abstract O publish(final R publishBatchRequest);

  protected abstract void handleError(final R publishBatchRequest, final Throwable throwable);

  protected abstract void handleResponse(final O publishBatchResult);

  protected abstract BiFunction<String, List<RequestEntryInternal>, R> supplierPublishRequest();

  private void doPublish(final R publishBatchRequest) {
    try {
      handleResponse(publish(publishDecorator.apply(publishBatchRequest)));
    } catch (final Exception ex) {
      handleError(publishBatchRequest, ex);
    }
  }

  private void publishBatch(final R publishBatchRequest) {
    if (queueProperty.isFifo()) {
      doPublish(publishBatchRequest);
    } else {
      try {
        CompletableFuture.runAsync(() -> doPublish(publishBatchRequest), executorService);
      } catch (final Exception ex) {
        handleError(publishBatchRequest, ex);
      }
    }
  }

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

  @SneakyThrows
  public void shutdown() {
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
  }

  private boolean requestsWaitedFor(final BlockingQueue<RequestEntry<E>> requests, final long batchingWindowInMs) {
    return Optional.ofNullable(requests.peek()).map(oldestPendingRequest -> {
      final long oldestEntryWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - oldestPendingRequest.getCreateTime());
      return oldestEntryWaitTime > batchingWindowInMs;
    }).orElse(false);
  }

  private boolean maxBatchSizeReached(final BlockingQueue<RequestEntry<E>> requests) {
    return requests.size() > queueProperty.getMaxBatchSize();
  }

  @SneakyThrows
  private void validatePayloadSize(final byte[] payload) {
    if (payload.length > BATCH_SIZE_BYTES_THRESHOLD) {
      final String value = new String(payload, StandardCharsets.UTF_8);
      final String message = String.format("The maximum allowed message size exceeding 256KB (262,144 bytes). Payload: %s", value);
      throw new IOException(message);
    }
  }

  private boolean canAddToBatch(final int batchSizeBytes, final int requestEntriesSize, final RequestEntry<E> request) {
    return (batchSizeBytes < BATCH_SIZE_BYTES_THRESHOLD)
      && (requestEntriesSize < queueProperty.getMaxBatchSize())
      && Objects.nonNull(request);
  }

  private boolean canAddPayload(final int batchSizeBytes) {
    return batchSizeBytes <= BATCH_SIZE_BYTES_THRESHOLD;
  }

  @SneakyThrows
  private Optional<R> createBatch(final BlockingQueue<RequestEntry<E>> requests) {
    final AtomicInteger batchSizeBytes = new AtomicInteger(0);
    final List<RequestEntryInternal> requestEntries = new LinkedList<>();

    while (canAddToBatch(batchSizeBytes.get(), requestEntries.size(), requests.peek())) {
      final RequestEntry<E> request = requests.peek();

      final byte[] payload = requestEntryInternalFactory.convertPayload(request);

      validatePayloadSize(payload);

      if (canAddPayload(batchSizeBytes.addAndGet(payload.length))) {
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

  @SneakyThrows
  public CompletableFuture<Void> await() {
    return CompletableFuture.runAsync(() -> {
      while (MapUtils.isNotEmpty(this.pendingRequests) ||
        CollectionUtils.isNotEmpty(this.queueRequests)) {
        sleep(queueProperty.getLinger());
      }
    });
  }

  @SneakyThrows
  private static void sleep(final long millis) {
    Thread.sleep(millis);
  }

}
// @formatter:on
