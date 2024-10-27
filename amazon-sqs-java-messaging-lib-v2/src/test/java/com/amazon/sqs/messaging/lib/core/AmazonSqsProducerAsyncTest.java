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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.core.helper.ConsumerHelper;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

// @formatter:off
@ExtendWith(MockitoExtension.class)
class AmazonSqsProducerAsyncTest {

  private AmazonSqsTemplate<Object> sqsTemplate;

  @Mock
  private SqsClient amazonSQS;

  @BeforeEach
  public void before() throws Exception {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .linger(50L)
      .maxBatchSize(10)
      .maximumPoolSize(10)
      .queueUrl("http://localhost/000000000000/queue")
      .build();

    sqsTemplate = new AmazonSqsTemplate<>(amazonSQS, queueProperty, new RingBufferBlockingQueue<>(1024));
  }

  @Test
  void testSuccess() {
    final String id = UUID.randomUUID().toString();

    final SendMessageBatchResultEntry publishBatchResultEntry = SendMessageBatchResultEntry.builder().id(id).build();

    final SendMessageBatchResponse publishBatchResult = SendMessageBatchResponse.builder()
      .successful(publishBatchResultEntry)
      .build();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(publishBatchResult);

    sqsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();

  }

  @Test
  void testFailure() {
    final String id = UUID.randomUUID().toString();

    final BatchResultErrorEntry batchResultErrorEntry = BatchResultErrorEntry.builder().id(id).build();

    final SendMessageBatchResponse publishBatchResult = SendMessageBatchResponse.builder()
      .failed(batchResultErrorEntry)
      .build();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(publishBatchResult);

    sqsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(null, result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();

  }

  @Test
  void testSuccessMultipleEntry() {

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenAnswer(invocation -> {
      final SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
      final List<SendMessageBatchResultEntry> resultEntries = request.entries().stream()
        .map(entry -> SendMessageBatchResultEntry.builder().id(entry.id()).build())
        .collect(Collectors.toList());;
      return SendMessageBatchResponse.builder().successful(resultEntries).build();
    });

    final ConsumerHelper<ResponseSuccessEntry> successCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(30000).forEach(entry -> {
      sqsTemplate.send(entry).addCallback(successCallback);
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(successCallback, timeout(300000).times(30000)).accept(any());
      verify(amazonSQS, atLeastOnce()).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();
  }

  @Test
  void testFailureMultipleEntry() {

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenAnswer(invocation -> {
      final SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
      final List<BatchResultErrorEntry> resultEntries = request.entries().stream()
        .map(entry -> BatchResultErrorEntry.builder().id(entry.id()).build())
        .collect(Collectors.toList());;
      return SendMessageBatchResponse.builder().failed(resultEntries).build();
    });

    final ConsumerHelper<ResponseFailEntry> failureCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(30000).forEach(entry -> {
      sqsTemplate.send(entry).addCallback(null, failureCallback);
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(failureCallback, timeout(300000).times(30000)).accept(any());
      verify(amazonSQS, atLeastOnce()).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();
  }

  @Test
  void testFailRiseRuntimeException() {
    final String id = UUID.randomUUID().toString();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(new RuntimeException());

    sqsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();

  }

  @Test
  void testFailRiseAwsServiceException() {
    final String id = UUID.randomUUID().toString();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(AwsServiceException.builder().awsErrorDetails(AwsErrorDetails.builder().build()).build());

    sqsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    sqsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();

  }

  @Test
  void testSuccessBlockingSubmissionPolicy() {
    final QueueProperty topicProperty = QueueProperty.builder()
      .fifo(false)
      .linger(50L)
      .maxBatchSize(1)
      .maximumPoolSize(1)
      .queueUrl("http://localhost/000000000000/queue")
      .build();

    final AmazonSqsTemplate<Object> sqsTemplate = new AmazonSqsTemplate<>(amazonSQS, topicProperty);

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenAnswer(invocation -> {
      while (true) {
        Thread.sleep(1);
      }
    });

    final ConsumerHelper<ResponseFailEntry> failureCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(2).forEach(entry -> {
      sqsTemplate.send(entry).addCallback(null, failureCallback);;
    });

    verify(failureCallback, timeout(40000).times(1)).accept(any());
    verify(amazonSQS, atLeastOnce()).sendMessageBatch(any(SendMessageBatchRequest.class));
  }

  private List<RequestEntry<Object>> entries(final int amount) {
    final LinkedList<RequestEntry<Object>> entries = new LinkedList<>();

    for (int i = 0; i < amount; i++) {
      entries.add(RequestEntry.builder()
        .withGroupId(UUID.randomUUID().toString())
        .withDeduplicationId(UUID.randomUUID().toString())
        .build());
    }

    return entries;
  }

}
// @formatter:on
