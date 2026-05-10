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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import com.amazon.sqs.messaging.lib.concurrent.ExecutorsProvider;
import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.fasterxml.jackson.databind.ObjectMapper;

// @formatter:off
public class AmazonSqsTemplate<E> extends AbstractAmazonSqsTemplate<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E> {

  private AmazonSqsTemplate(
      final AmazonSQS amazonSqsClient,
      final QueueProperty queueProperty,
      final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests,
      final BlockingQueue<RequestEntry<E>> queueRequests,
      final ObjectMapper objectMapper,
      final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    super(
      new AmazonSqsProducer<>(pendingRequests, queueRequests, ExecutorsProvider.getExecutorService()),
      new AmazonSqsConsumer<>(amazonSqsClient, queueProperty, objectMapper, pendingRequests, queueRequests, getAmazonSqsThreadPoolExecutor(queueProperty), publishDecorator)
    );
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty) {
    this(amazonSqsClient, queueProperty, UnaryOperator.identity());
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new ObjectMapper(), publishDecorator);
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests) {
    this(amazonSqsClient, queueProperty, queueRequests, UnaryOperator.identity());
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, queueRequests, new ObjectMapper(), publishDecorator);
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, objectMapper, UnaryOperator.identity());
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new RingBufferBlockingQueue<>(queueProperty.getMaximumPoolSize() * queueProperty.getMaxBatchSize()), objectMapper, publishDecorator);
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, queueRequests, objectMapper, UnaryOperator.identity());
  }

  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new ConcurrentHashMap<>(), queueRequests, objectMapper, publishDecorator);
  }

}
// @formatter:on
