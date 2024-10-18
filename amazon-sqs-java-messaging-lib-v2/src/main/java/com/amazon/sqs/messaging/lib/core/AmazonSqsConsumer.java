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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

// @formatter:off
@SuppressWarnings("java:S6204")
class AmazonSqsConsumer<E> extends AbstractAmazonSqsConsumer<SqsClient, SendMessageBatchRequest, SendMessageBatchResponse, E> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmazonSqsConsumer.class);

  private static final MessageAttributes messageAttributes = new MessageAttributes();

  public AmazonSqsConsumer(
      final SqsClient amazonSqsClient,
      final QueueProperty queueProperty,
      final ObjectMapper objectMapper,
      final ConcurrentMap<String, ListenableFutureRegistry> pendingRequests,
      final BlockingQueue<RequestEntry<E>> queueRequests,
      final ExecutorService executorService,
      final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    super(amazonSqsClient, queueProperty, objectMapper, pendingRequests, queueRequests, executorService, publishDecorator);
  }

  @Override
  protected SendMessageBatchResponse publish(final SendMessageBatchRequest publishBatchRequest) {
    return amazonSqsClient.sendMessageBatch(publishBatchRequest);
  }

  @Override
  protected BiFunction<String, List<RequestEntry<E>>, SendMessageBatchRequest> supplierPublishRequest() {
    return (queueUrl, requestEntries) -> {
      final List<SendMessageBatchRequestEntry> entries = requestEntries.stream()
        .map(entry -> SendMessageBatchRequestEntry.builder()
          .id(entry.getId())
          .messageGroupId(StringUtils.isNotBlank(entry.getGroupId()) ? entry.getGroupId() : null)
          .messageDeduplicationId(StringUtils.isNotBlank(entry.getDeduplicationId()) ? entry.getDeduplicationId() : null)
          .messageAttributes(messageAttributes.messageAttributes(entry.getMessageHeaders()))
          .messageBody(convertPayload(entry.getValue()))
          .build())
        .collect(Collectors.toList());
      return SendMessageBatchRequest.builder().entries(entries).queueUrl(queueUrl).build();
    };
  }

  @Override
  protected void handleError(final SendMessageBatchRequest publishBatchRequest, final Throwable throwable) {
    final String code = throwable instanceof AwsServiceException ? AwsServiceException.class.cast(throwable).awsErrorDetails().errorCode() : "000";
    final String message = throwable instanceof AwsServiceException ? AwsServiceException.class.cast(throwable).awsErrorDetails().errorMessage() : throwable.getMessage();

    LOGGER.error(throwable.getMessage(), throwable);

    publishBatchRequest.entries().forEach(entry -> {
      final ListenableFutureRegistry listenableFuture = pendingRequests.remove(entry.id());
      listenableFuture.fail(ResponseFailEntry.builder()
        .withId(entry.id())
        .withCode(code)
        .withMessage(message)
        .withSenderFault(true)
        .build());
    });
  }

  @Override
  protected void handleResponse(final SendMessageBatchResponse publishBatchResult) {
    publishBatchResult.successful().forEach(entry -> {
      final ListenableFutureRegistry listenableFuture = pendingRequests.remove(entry.id());
      listenableFuture.success(ResponseSuccessEntry.builder()
        .withId(entry.id())
        .withMessageId(entry.messageId())
        .withSequenceNumber(entry.sequenceNumber())
        .build());
    });

    publishBatchResult.failed().forEach(entry -> {
      final ListenableFutureRegistry listenableFuture = pendingRequests.remove(entry.id());
      listenableFuture.fail(ResponseFailEntry.builder()
        .withId(entry.id())
        .withCode(entry.code())
        .withMessage(entry.message())
        .withSenderFault(entry.senderFault())
        .build());
    });
  }

}
// @formatter:on
