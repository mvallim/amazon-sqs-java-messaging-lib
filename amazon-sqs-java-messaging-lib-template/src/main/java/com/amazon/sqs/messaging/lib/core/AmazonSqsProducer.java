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

import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

/**
 * Producer interface for Amazon SQS messaging. Implementations enqueue request
 * entries for batch publishing and track pending requests for asynchronous
 * completion.
 *
 * @param <E> the request entry payload type
 */
public interface AmazonSqsProducer<E> {

  /**
   * Sends a request entry for asynchronous publishing to an SQS topic.
   *
   * @param requestEntry the request entry containing the message payload and
   *                     metadata
   * @return a {@link ListenableFuture} that completes when the request is
   *         processed
   */
  public ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> send(final RequestEntry<E> requestEntry);

  /**
   * Shuts down the producer, preventing any further messages from being accepted.
   */
  public void shutdown();

}
// @formatter:on
