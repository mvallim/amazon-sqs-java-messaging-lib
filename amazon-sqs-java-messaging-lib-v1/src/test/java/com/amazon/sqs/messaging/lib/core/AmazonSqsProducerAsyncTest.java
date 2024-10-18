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

import org.apache.commons.lang3.RandomStringUtils;
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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;

// @formatter:off
@ExtendWith(MockitoExtension.class)
class AmazonSqsProducerAsyncTest {

  private AmazonSqsTemplate<Object> snsTemplate;

  @Mock
  private AmazonSQS amazonSQS;

  @BeforeEach
  public void before() throws Exception {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .linger(50L)
      .maxBatchSize(10)
      .maximumPoolSize(10)
      .queueUrl("http://localhost/000000000000/queue")
      .build();
    snsTemplate = new AmazonSqsTemplate<>(amazonSQS, queueProperty, new RingBufferBlockingQueue<>(1024));
  }

  @Test
  void testSuccess() {
    final String id = UUID.randomUUID().toString();

    final SendMessageBatchResultEntry publishBatchResultEntry = new SendMessageBatchResultEntry();
    publishBatchResultEntry.setId(id);

    final SendMessageBatchResult publishBatchResult = new SendMessageBatchResult();
    publishBatchResult.getSuccessful().add(publishBatchResultEntry);

    when(amazonSQS.sendMessageBatch(any())).thenReturn(publishBatchResult);

    snsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    snsTemplate.await().thenAccept(result -> {
      snsTemplate.shutdown();
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any());
    }).join();

  }

  @Test
  void testFailure() {
    final String id = UUID.randomUUID().toString();

    final BatchResultErrorEntry batchResultErrorEntry = new BatchResultErrorEntry();
    batchResultErrorEntry.setId(id);

    final SendMessageBatchResult publishBatchResult = new SendMessageBatchResult();
    publishBatchResult.getFailed().add(batchResultErrorEntry);

    when(amazonSQS.sendMessageBatch(any())).thenReturn(publishBatchResult);

    snsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(null, result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    snsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any());
    }).join();

  }

  @Test
  void testSuccessMultipleEntry() {

    when(amazonSQS.sendMessageBatch(any())).thenAnswer(invocation -> {
      final SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
      final List<SendMessageBatchResultEntry> resultEntries = request.getEntries().stream()
        .map(entry -> new SendMessageBatchResultEntry().withId(entry.getId()))
        .collect(Collectors.toList());
      return new SendMessageBatchResult().withSuccessful(resultEntries);
    });

    final ConsumerHelper<ResponseSuccessEntry> successCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(30000).forEach(entry -> {
      snsTemplate.send(entry).addCallback(successCallback);
    });

    snsTemplate.await().thenAccept(result -> {
      verify(successCallback, timeout(300000).times(30000)).accept(any());
      verify(amazonSQS, atLeastOnce()).sendMessageBatch(any());
    }).join();
  }

  @Test
  void testFailureMultipleEntry() {

    when(amazonSQS.sendMessageBatch(any())).thenAnswer(invocation -> {
      final SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
      final List<BatchResultErrorEntry> resultEntries = request.getEntries().stream()
        .map(entry -> new BatchResultErrorEntry().withId(entry.getId()))
        .collect(Collectors.toList());;
      return new SendMessageBatchResult().withFailed(resultEntries);
    });

    final ConsumerHelper<ResponseFailEntry> failureCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(30000).forEach(entry -> {
      snsTemplate.send(entry).addCallback(null, failureCallback);
    });

    snsTemplate.await().thenAccept(result -> {
      verify(failureCallback, timeout(300000).times(30000)).accept(any());
      verify(amazonSQS, atLeastOnce()).sendMessageBatch(any());
    }).join();
  }

  @Test
  void testFailRiseRuntimeException() {
    final String id = UUID.randomUUID().toString();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(new RuntimeException());

    snsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    snsTemplate.await().thenAccept(result -> {
      verify(amazonSQS, timeout(10000).times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }).join();

  }

  @Test
  void testFailRiseAwsServiceException() {
    final String id = UUID.randomUUID().toString();

    when(amazonSQS.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(new AmazonServiceException("error"));

    snsTemplate.send(RequestEntry.builder().withId(id).build()).addCallback(result -> {
      assertThat(result, notNullValue());
      assertThat(result.getId(), is(id));
    });

    snsTemplate.await().thenAccept(result -> {
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

    final AmazonSqsTemplate<Object> snsTemplate = new AmazonSqsTemplate<>(amazonSQS, topicProperty);

    when(amazonSQS.sendMessageBatch(any())).thenAnswer(invocation -> {
      while (true) {
        Thread.sleep(1);
      }
    });

    final ConsumerHelper<ResponseFailEntry> failureCallback = spy(new ConsumerHelper<>(result -> {
      assertThat(result, notNullValue());
    }));

    entries(2).forEach(entry -> {
      snsTemplate.send(entry).addCallback(null, failureCallback);;
    });

    verify(failureCallback, timeout(40000).times(1)).accept(any());
    verify(amazonSQS, atLeastOnce()).sendMessageBatch(any());
  }

  private List<RequestEntry<Object>> entries(final int amount) {
    final LinkedList<RequestEntry<Object>> entries = new LinkedList<>();

    for (int i = 0; i < amount; i++) {
      entries.add(RequestEntry.builder()
        .withId(RandomStringUtils.randomAlphabetic(36))
        .withGroupId(UUID.randomUUID().toString())
        .withDeduplicationId(UUID.randomUUID().toString())
        .build());
    }

    return entries;
  }

}
// @formatter:on
