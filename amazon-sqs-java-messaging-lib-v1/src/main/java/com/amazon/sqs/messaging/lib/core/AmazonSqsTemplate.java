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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;

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
   * @param amazonSqsClient  the v1 {@link AmazonSQS} client
   * @param queueProperty    the queue configuration
   * @return a new builder instance
   */
  public static <E> Builder<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E, AmazonSqsTemplate<E>> builder(
      final AmazonSQS amazonSqsClient,
      final QueueProperty queueProperty) {
    return new Builder<>(AmazonSqsTemplate::new, amazonSqsClient, queueProperty);
  }

}
// @formatter:on
