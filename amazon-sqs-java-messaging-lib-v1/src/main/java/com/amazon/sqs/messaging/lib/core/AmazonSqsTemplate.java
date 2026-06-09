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
import java.util.function.UnaryOperator;

import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.metrics.AmazonSqsConsumerMetricsDecorator;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
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
public class AmazonSqsTemplate<E> extends AbstractAmazonSqsTemplate<SendMessageBatchRequest, SendMessageBatchResult, E> {

  private AmazonSqsTemplate(final Builder<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E, AmazonSqsTemplate<E>> builder) {
    super(
      new AmazonSqsProducerImpl<>(
        builder.getPendingRequests(),
        builder.getQueueRequests()
      ),
      new AmazonSqsConsumerMetricsDecorator(
        new AmazonSqsConsumerImpl<>(
          builder.getAmazonSqsClient(),
          builder.getQueueProperty(),
          builder.getObjectMapper(),
          builder.getPendingRequests(),
          builder.getQueueRequests(),
          getExecutorService(builder.getQueueProperty(), builder.getMeterRegistry()),
          builder.getPublishDecorator()
        ),
        builder.getQueueProperty(),
        builder.getMeterRegistry()
      )
    );
  }

  /**
   * Creates a new builder for constructing an {@link AmazonSnsTemplate}.
   *
   * @param <E>              the request entry payload type
   * @param amazonSqsClient  the v1 {@link AmazonSQS} client
   * @param queueProperty    the queue configuration
   * @return a new builder instance
   */
  public static <E> Builder<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E, AmazonSqsTemplate<E>> builder(
      final AmazonSQS amazonSqsClient,
      final QueueProperty queueProperty) {
    return new Builder<>(AmazonSqsTemplate::new, amazonSqsClient, queueProperty);
  }

  /**
   * Creates a template with default object mapper and identity publish decorator.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} instead
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty) {
    this(amazonSqsClient, queueProperty, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom publish decorator and default object mapper.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} and {@link Builder#publishDecorator(UnaryOperator)} instead
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param publishDecorator a decorator for batch publish requests
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new ObjectMapper(), publishDecorator);
  }

  /**
   * Creates a template with a custom request queue, default object mapper,
   * and identity publish decorator.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} and {@link Builder#queueRequests(BlockingQueue)} instead
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param queueRequests   the blocking queue for incoming requests
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests) {
    this(amazonSqsClient, queueProperty, queueRequests, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom request queue and publish decorator.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} with {@link Builder#queueRequests(BlockingQueue)} and {@link Builder#publishDecorator(UnaryOperator)} instead
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param queueRequests    the blocking queue for incoming requests
   * @param publishDecorator a decorator for batch publish requests
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, queueRequests, new ObjectMapper(), publishDecorator);
  }

  /**
   * Creates a template with a custom object mapper and identity publish decorator.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} and {@link Builder#objectMapper(ObjectMapper)} instead
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param objectMapper    the JSON object mapper
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, objectMapper, UnaryOperator.identity());
  }

  /**
   * Creates a template with a custom object mapper and publish decorator.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} with {@link Builder#objectMapper(ObjectMapper)} and {@link Builder#publishDecorator(UnaryOperator)} instead
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param objectMapper     the JSON object mapper
   * @param publishDecorator a decorator for batch publish requests
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(amazonSqsClient, queueProperty, new RingBufferBlockingQueue<>(queueProperty.getMaximumPoolSize() * queueProperty.getMaxBatchSize()), objectMapper, publishDecorator);
  }

  /**
   * Creates a template with a custom request queue and object mapper.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} with {@link Builder#queueRequests(BlockingQueue)} and {@link Builder#objectMapper(ObjectMapper)} instead
   *
   * @param amazonSqsClient the AWS SDK v1 SQS client
   * @param queueProperty   the queue configuration properties
   * @param queueRequests   the blocking queue for incoming requests
   * @param objectMapper    the JSON object mapper
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper) {
    this(amazonSqsClient, queueProperty, queueRequests, objectMapper, UnaryOperator.identity());
  }

  /**
   * Creates a template with full custom configuration.
   *
   * @deprecated since 1.3.0, use {@link #builder(AmazonSQS, QueueProperty)} with builder setters instead
   *
   * @param amazonSqsClient  the AWS SDK v1 SQS client
   * @param queueProperty    the queue configuration properties
   * @param queueRequests    the blocking queue for incoming requests
   * @param objectMapper     the JSON object mapper
   * @param publishDecorator a decorator for batch publish requests
   */
  @Deprecated
  public AmazonSqsTemplate(final AmazonSQS amazonSqsClient, final QueueProperty queueProperty, final BlockingQueue<RequestEntry<E>> queueRequests, final ObjectMapper objectMapper, final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    this(AmazonSqsTemplate.<E>builder(amazonSqsClient, queueProperty)
      .queueRequests(queueRequests)
      .objectMapper(objectMapper)
      .publishDecorator(publishDecorator)
    );
  }

}
// @formatter:on
