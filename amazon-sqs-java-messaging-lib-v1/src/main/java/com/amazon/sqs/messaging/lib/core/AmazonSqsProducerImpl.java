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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

// @formatter:off
/**
 * Amazon SQS v1 producer implementation. Delegates to {@link AbstractAmazonSqsProducer}.
 *
 * @param <E> the request entry payload type
 */
class AmazonSqsProducerImpl<E> extends AbstractAmazonSqsProducer<E> {

  /**
   * Constructs a v1 SQS producer.
   *
   * @param pendingRequests the map of pending requests
   * @param queueRequests   the blocking queue of incoming requests
   */
  public AmazonSqsProducerImpl(
      final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests,
      final BlockingQueue<RequestEntry<E>> queueRequests) {
    super(pendingRequests, queueRequests);
  }

}
// @formatter:on
