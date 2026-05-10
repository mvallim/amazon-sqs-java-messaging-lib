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

import java.util.concurrent.CompletableFuture;

import com.amazon.sqs.messaging.lib.concurrent.AmazonSqsThreadPoolExecutor;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Abstract base template for Amazon SQS messaging operations. Provides a high-level
 * abstraction for sending messages and managing the lifecycle of the underlying
 * producer and consumer components.
 *
 * @param <C> the Amazon SQS client type
 * @param <R> the publish batch request type
 * @param <O> the publish batch response type
 * @param <E> the request entry payload type
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractAmazonSqsTemplate<C, R, O, E> {

  private final AbstractAmazonSqsProducer<E> amazonSqsProducer;

  private final AbstractAmazonSqsConsumer<C, R, O, E> amazonSqsConsumer;

  /**
   * Sends a message to the Amazon SQS queue.
   *
   * @param requestEntry the request entry containing the message payload and headers
   * @return a {@link ListenableFuture} that can be used to track the success or failure of the send operation
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
   * @return a {@link CompletableFuture} that completes when all pending requests have been processed
   */
  public CompletableFuture<Void> await() {
    return amazonSqsConsumer.await();
  }

  /**
   * Creates an {@link AmazonSqsThreadPoolExecutor} configured for the given queue property.
   * For FIFO queues, a single-thread executor is used to preserve message ordering.
   *
   * @param queueProperty the queue configuration properties
   * @return a configured thread pool executor
   */
  protected static AmazonSqsThreadPoolExecutor getAmazonSqsThreadPoolExecutor(final QueueProperty queueProperty) {
    if (queueProperty.isFifo()) {
      return new AmazonSqsThreadPoolExecutor(1);
    } else {
      return new AmazonSqsThreadPoolExecutor(queueProperty.getMaximumPoolSize());
    }
  }

}