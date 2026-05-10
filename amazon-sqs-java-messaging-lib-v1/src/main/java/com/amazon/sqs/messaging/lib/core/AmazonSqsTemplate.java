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
/**
 * Amazon SQS v1 template providing convenience constructors for creating a fully
 * configured messaging pipeline using the AWS SDK v1 {@link AmazonSQS} client.
 *
 * @param <E> the request entry payload type
 */
public class AmazonSqsTemplate<E> extends AbstractAmazonSqsTemplate<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E> {

  /**
   * Primary constructor that wires the producer and consumer together.
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param pendingRequests  the map of pending requests
   * @param queueRequests    the blocking queue of incoming requests
   * @param objectMapper     the JSON object mapper
   * @param publishDecorator a decorator for batch publish requests
   */
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

  /**
   * Creates a template with default object mapper and identity publish decorator.
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty) {
    this(amazonSqsClient, queueProperty, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom publish decorator and default object mapper.
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param publishDecorator a decorator for batch publish requests
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new ObjectMapper(), publishDecorator);
  }

  /**
   * Creates a template with a custom request queue, default object mapper,
   * and identity publish decorator.
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param queueRequests   the blocking queue for incoming requests
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests) {
    this(amazonSqsClient, queueProperty, queueRequests, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom request queue and publish decorator.
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param queueRequests    the blocking queue for incoming requests
   * @param publishDecorator a decorator for batch publish requests
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, queueRequests, new ObjectMapper(), publishDecorator);
  }

  /**
   * Creates a template with a custom object mapper and identity publish decorator.
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param objectMapper    the JSON object mapper
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, objectMapper, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom object mapper and publish decorator.
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param objectMapper     the JSON object mapper
   * @param publishDecorator a decorator for batch publish requests
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new RingBufferBlockingQueue<>(queueProperty.getMaximumPoolSize() * queueProperty.getMaxBatchSize()), objectMapper, publishDecorator);
  }

  /**
   * Creates a template with a custom request queue and object mapper.
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param queueRequests   the blocking queue for incoming requests
   * @param objectMapper    the JSON object mapper
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, queueRequests, objectMapper, UnaryOperator.identity());
  }

  /**
   * Creates a template with full custom configuration.
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param queueRequests    the blocking queue for incoming requests
   * @param objectMapper     the JSON object mapper
   * @param publishDecorator a decorator for batch publish requests
   */
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new ConcurrentHashMap<>(), queueRequests, objectMapper, publishDecorator);
  }

}
// @formatter:on
