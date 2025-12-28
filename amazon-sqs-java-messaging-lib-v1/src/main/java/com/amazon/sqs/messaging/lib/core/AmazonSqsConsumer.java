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
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.messaging.lib.core.RequestEntryInternalFactory.RequestEntryInternal;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.fasterxml.jackson.databind.ObjectMapper;

// @formatter:off
@SuppressWarnings("java:S6204")
class AmazonSqsConsumer<E> extends AbstractAmazonSqsConsumer<AmazonSQS, SendMessageBatchRequest, SendMessageBatchResult, E> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmazonSqsConsumer.class);

  private static final MessageAttributes messageAttributes = new MessageAttributes();

  public AmazonSqsConsumer(
    final AmazonSQS amazonSqsClient,
    final QueueProperty queueProperty,
    final ObjectMapper objectMapper,
    final ConcurrentMap<String, ListenableFutureRegistry> pendingRequests,
    final BlockingQueue<RequestEntry<E>> topicRequests,
    final ExecutorService executorService,
    final UnaryOperator<SendMessageBatchRequest> publishDecorator) {
    super(amazonSqsClient, queueProperty, objectMapper, pendingRequests, topicRequests, executorService, publishDecorator);
  }

  @Override
  protected SendMessageBatchResult publish(final SendMessageBatchRequest publishBatchRequest) {
    return amazonSqsClient.sendMessageBatch(publishBatchRequest);
  }

  @Override
  protected BiFunction<String, List<RequestEntryInternal>, SendMessageBatchRequest> supplierPublishRequest() {
    return (queueUrl, requestEntries) -> {
      final List<SendMessageBatchRequestEntry> entries = requestEntries.stream()
        .map(entry -> new SendMessageBatchRequestEntry()
          .withId(entry.getId())
          .withMessageGroupId(StringUtils.isNotBlank(entry.getGroupId()) ? entry.getGroupId() : null)
          .withMessageDeduplicationId(StringUtils.isNotBlank(entry.getDeduplicationId()) ? entry.getDeduplicationId() : null)
          .withMessageAttributes(messageAttributes.messageAttributes(entry.getMessageHeaders()))
          .withMessageBody(entry.getMessage()))
        .collect(Collectors.toList());
      return new SendMessageBatchRequest().withEntries(entries).withQueueUrl(queueUrl);
    };
  }

  @Override
  protected void handleError(final SendMessageBatchRequest publishBatchRequest, final Throwable throwable) {
    final String code = throwable instanceof AmazonServiceException ? AmazonServiceException.class.cast(throwable).getErrorCode() : "000";
    final String message = throwable instanceof AmazonServiceException ? AmazonServiceException.class.cast(throwable).getErrorMessage() : throwable.getMessage();

    LOGGER.error("Error processing batch request: {}", message, throwable);

    publishBatchRequest.getEntries().forEach(entry -> {
      Optional.ofNullable(pendingRequests.remove(entry.getId())).ifPresent(listenableFuture -> {
        listenableFuture.fail(ResponseFailEntry.builder()
          .withId(entry.getId())
          .withCode(code)
          .withMessage(message)
          .withSenderFault(true)
          .build());
      });
    });
  }

  @Override
  protected void handleResponse(final SendMessageBatchResult publishBatchResult) {
    publishBatchResult.getSuccessful().forEach(entry -> {
      Optional.ofNullable(pendingRequests.remove(entry.getId())).ifPresent(listenableFuture -> {
        listenableFuture.success(ResponseSuccessEntry.builder()
          .withId(entry.getId())
          .withMessageId(entry.getMessageId())
          .withSequenceNumber(entry.getSequenceNumber())
          .build());
      });
    });

    publishBatchResult.getFailed().forEach(entry -> {
      Optional.ofNullable(pendingRequests.remove(entry.getId())).ifPresent(listenableFuture -> {
        listenableFuture.fail(ResponseFailEntry.builder()
          .withId(entry.getId())
          .withCode(entry.getCode())
          .withMessage(entry.getMessage())
          .withSenderFault(entry.getSenderFault())
          .build());
      });
    });
  }

}
// @formatter:on
