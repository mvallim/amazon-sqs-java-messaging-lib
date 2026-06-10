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

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.amazon.sqs.messaging.lib.concurrent.AmazonSqsThreadPoolExecutor;
import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.metrics.BlockingQueueMetricsDecorator;
import com.amazon.sqs.messaging.lib.metrics.ExecutorServiceMetricsDecorator;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

//@formatter:off
/**
 * Abstract base template for Amazon SQS messaging operations. Provides a
 * high-level abstraction for sending messages and managing the lifecycle of the
 * underlying producer and consumer components.
 *
 * @param <R> the publish batch request type
 * @param <O> the publish batch response type
 * @param <E> the request entry payload type
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractAmazonSqsTemplate<R, O, E> {

  private final AmazonSqsProducer<E> amazonSqsProducer;

  private final AmazonSqsConsumer<R, O> amazonSqsConsumer;

  /**
   * Sends a message to the Amazon SQS queue.
   *
   * @param requestEntry the request entry containing the message payload and
   *                     headers
   * @return a {@link ListenableFuture} that can be used to track the success or
   *         failure of the send operation
   */
  public ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> send(final RequestEntry<E> requestEntry) {
    return amazonSqsProducer.send(requestEntry);
  }

  /**
   * Shuts down both the producer and consumer, releasing all resources.
   */
  public void shutdown() {
    amazonSqsProducer.shutdown();
    amazonSqsConsumer.shutdown();
  }

  /**
   * Waits for all pending requests to be processed.
   *
   * @return a {@link CompletableFuture} that completes when all pending requests
   *         have been processed
   */
  public CompletableFuture<Void> await() {
    return amazonSqsConsumer.await();
  }

  /**
   * Creates an {@link AmazonSqsThreadPoolExecutor} configured for the given queue
   * property. For FIFO queues, a single-thread executor is used to preserve
   * message ordering.
   *
   * @param queueProperty the queue configuration properties
   * @return a configured thread pool executor
   */
  protected static ExecutorService getExecutorService(final QueueProperty queueProperty, final MeterRegistry meterRegistry) {
    return queueProperty.isFifo()
      ? new ExecutorServiceMetricsDecorator(
          new AmazonSqsThreadPoolExecutor(1),
          meterRegistry,
          queueProperty.getQueueUrl()
        )
      : new ExecutorServiceMetricsDecorator(
          new AmazonSqsThreadPoolExecutor(queueProperty.getMaximumPoolSize()),
          meterRegistry,
          queueProperty.getQueueUrl()
        );
  }

  @Getter
  public static final class Builder<C, R, O, E, T extends AbstractAmazonSqsTemplate<R, O, E>> {

    /**
     * The Amazon SQS client used for publishing.
     */
    private final C amazonSqsClient;

    /**
     * The queue configuration properties.
     */
    private final QueueProperty queueProperty;

    /**
     * Map of pending requests tracked by request ID for asynchronous completion.
     */
    private ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests = new ConcurrentHashMap<>();

    /**
     * The blocking queue for buffering queue requests before batching.
     */
    private BlockingQueue<RequestEntry<E>> queueRequests;

    /**
     * The Jackson ObjectMapper for serializing payloads.
     */
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Decorator function applied to the publish batch request before sending.
     */
    private UnaryOperator<R> publishDecorator = UnaryOperator.identity();

    /**
     * The Micrometer meter registry for collecting metrics.
     */
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();

    /**
     * Internal constructor reference used to create the template instance.
     */
    private final Function<Builder<C, R, O, E, T>, T> constructor;

    /**
     * Creates a new builder with the required constructor reference, client, and topic.
     *
     * @param constructor     the constructor function for creating the template instance
     * @param amazonSqsClient the Amazon SQS client
     * @param queueProperty   the queue configuration properties
     */
    Builder(final Function<Builder<C, R, O, E, T>, T> constructor, final C amazonSqsClient, final QueueProperty queueProperty) {
      this.amazonSqsClient = Objects.requireNonNull(amazonSqsClient, "amazonSqsClient");
      this.queueProperty = Objects.requireNonNull(queueProperty, "queueProperty");
      this.constructor = Objects.requireNonNull(constructor, "constructor");
    }

    /**
     * Sets the map of pending requests.
     *
     * @param pendingRequests the concurrent map keyed by request ID
     * @return this builder
     */
    public Builder<C, R, O, E, T> pendingRequests(final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests) {
      this.pendingRequests = Objects.requireNonNull(pendingRequests, "pendingRequests");
      return this;
    }

    /**
     * Sets the blocking queue for topic requests.
     *
     * @param queueRequests the blocking queue for queue requests
     * @return this builder
     */
    public Builder<C, R, O, E, T> queueRequests(final BlockingQueue<RequestEntry<E>> queueRequests) {
      this.queueRequests = Objects.requireNonNull(queueRequests, "queueRequests");
      return this;
    }

    /**
     * Sets the Jackson ObjectMapper for serializing payloads.
     *
     * @param objectMapper the Jackson ObjectMapper
     * @return this builder
     */
    public Builder<C, R, O, E, T> objectMapper(final ObjectMapper objectMapper) {
      this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
      return this;
    }

    /**
     * Sets the decorator for the publish batch request.
     *
     * @param publishDecorator the unary operator to apply before publishing
     * @return this builder
     */
    public Builder<C, R, O, E, T> publishDecorator(final UnaryOperator<R> publishDecorator) {
      this.publishDecorator = Objects.requireNonNull(publishDecorator, "publishDecorator");
      return this;
    }

    /**
     * Sets the Micrometer meter registry.
     *
     * @param meterRegistry the meter registry for metrics
     * @return this builder
     */
    public Builder<C, R, O, E, T> meterRegistry(final MeterRegistry meterRegistry) {
      this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
      return this;
    }

    /**
     * Builds the template instance. If no topic requests queue was provided, a default
     * {@link RingBufferBlockingQueue} is created. The queue is then decorated with
     * {@link BlockingQueueMetricsDecorator}.
     *
     * @return the constructed template instance
     */
    public T build() {
      if (Objects.isNull(queueRequests)) {
        queueRequests = new RingBufferBlockingQueue<>(queueProperty.getMaximumPoolSize() * queueProperty.getMaxBatchSize());
      }

      queueRequests = new BlockingQueueMetricsDecorator<>(queueRequests, meterRegistry, queueProperty.getQueueUrl());

      return constructor.apply(this);
    }

  }

}