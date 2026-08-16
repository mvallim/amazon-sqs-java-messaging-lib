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

import com.amazon.sqs.messaging.lib.metrics.AmazonSqsConsumerMetricsDecorator;
import com.amazon.sqs.messaging.lib.model.QueueProperty;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

// @formatter:off
/**
 * Amazon SQS v2 template providing convenience constructors for creating a fully
 * configured messaging pipeline using the AWS SDK v2 {@link SqsClient}.
 *
 * @param <E> the request entry payload type
 */
public class AmazonSqsTemplate<E> extends AbstractAmazonSqsTemplate<SendMessageBatchRequest, SendMessageBatchResponse, E> {

  private AmazonSqsTemplate(final Builder<SqsClient, SendMessageBatchRequest, SendMessageBatchResponse, E, AmazonSqsTemplate<E>> builder) {
    super(
      new AmazonSqsProducerImpl<>(
        builder.getPendingRequests(),
        builder.getQueueRequests()
      ),
      new AmazonSqsConsumerMetricsDecorator(
        new AmazonSqsConsumerImpl<>(
          builder.getAmazonSqsClient(),
          builder.getQueueProperty(),
          builder.getJsonMapper(),
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
   * Creates a new builder for constructing an {@link AmazonSqsTemplate}.
   *
   * @param <E>              the request entry payload type
   * @param amazonSqsClient  the v2 {@link SqsClient} client
   * @param queueProperty    the queue configuration
   * @return a new builder instance
   */
  public static <E> Builder<SqsClient, SendMessageBatchRequest, SendMessageBatchResponse, E, AmazonSqsTemplate<E>> builder(
      final SqsClient amazonSqsClient,
      final QueueProperty queueProperty) {
    return new Builder<>(AmazonSqsTemplate::new, amazonSqsClient, queueProperty);
  }

}
// @formatter:on
