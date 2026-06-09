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

import java.util.concurrent.CompletableFuture;

/**
 * Consumer interface for Amazon SNS messaging. Implementations handle batch
 * publishing of requests and dispatching of responses or errors to pending
 * request futures.
 *
 * @param <R> the publish batch request type
 * @param <O> the publish batch result type
 */
public interface AmazonSqsConsumer<R, O> {

  /**
   * Publishes a batch of messages to Amazon SQS.
   *
   * @param publishBatchRequest the batch publish request
   * @return the batch publish response
   */
  public O publish(final R publishBatchRequest);

  /**
   * Handles errors that occur during batch publishing.
   *
   * @param publishBatchRequest the batch publish request that failed
   * @param throwable           the exception that occurred
   */
  public void handleError(final R publishBatchRequest, final Throwable throwable);

  /**
   * Handles the successful response from a batch publish operation, notifying
   * pending futures of success or failure per entry.
   *
   * @param publishBatchResult the batch publish result
   */
  public void handleResponse(final O publishBatchResult);

  /**
   * Shuts down the consumer, waiting up to 60 seconds for both the scheduled and
   * worker executor services to terminate.
   */
  public void shutdown();

  /**
   * Returns a {@link CompletableFuture} that completes once all pending requests
   * have been processed (i.e., both the pending requests map and the topic
   * requests queue are empty).
   *
   * @return a future that completes when all requests are drained
   */
  public CompletableFuture<Void> await();

}
