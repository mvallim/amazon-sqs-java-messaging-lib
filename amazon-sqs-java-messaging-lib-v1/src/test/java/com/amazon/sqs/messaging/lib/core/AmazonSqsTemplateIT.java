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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.amazon.sqs.messaging.lib.concurrent.RingBufferBlockingQueue;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import lombok.SneakyThrows;

// @formatter:off
@Testcontainers
@SuppressWarnings("resource")
class AmazonSqsTemplateIT {

  @Container
  static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4.0"))
    .withEnv("LOCALSTACK_HOST", "localhost:4566")
    .withEnv("SQS_ENDPOINT_STRATEGY", "dynamic")
    .withReuse(true)
    .withServices(Service.SQS);

  private static AmazonSQS sqsClient;

  private static String standardQueueUrl;

  private static String fifoQueueUrl;

  @BeforeAll
  static void setup() {
    sqsClient = AmazonSQSClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(localstack.getEndpoint().toString(), localstack.getRegion()))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
      .build();

    standardQueueUrl = sqsClient.createQueue(
      new CreateQueueRequest().withQueueName("integration-test-standard-" + UUID.randomUUID())
    ).getQueueUrl();

    final Map<String, String> fifoAttributes = new HashMap<>();
    fifoAttributes.put(QueueAttributeName.FifoQueue.toString(), "true");
    fifoAttributes.put(QueueAttributeName.ContentBasedDeduplication.toString(), "true");

    fifoQueueUrl = sqsClient.createQueue(
      new CreateQueueRequest()
        .withQueueName("integration-test-fifo-" + UUID.randomUUID() + ".fifo")
        .withAttributes(fifoAttributes)
    ).getQueueUrl();
  }

  @AfterAll
  static void teardown() {
    if (Objects.nonNull(sqsClient)) {
      sqsClient.shutdown();
    }

    if (Objects.nonNull(localstack)) {
      localstack.close();
    }
  }

  @BeforeEach
  void before() {
    purgeQueue(standardQueueUrl);
    purgeQueue(fifoQueueUrl);
  }

  private AmazonSqsTemplate<Object> createTemplate(
      final String queueUrl,
      final boolean fifo,
      final long linger,
      final int maxBatchSize,
      final int maxPoolSize) {

    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(fifo)
      .linger(linger)
      .maxBatchSize(maxBatchSize)
      .maximumPoolSize(maxPoolSize)
      .queueUrl(queueUrl)
      .build();

    return new AmazonSqsTemplate<>(sqsClient, queueProperty, new RingBufferBlockingQueue<>(1024));
  }

  private void purgeQueue(final String queueUrl) {
    sqsClient.purgeQueue(new PurgeQueueRequest(queueUrl));
  }

  private ReceiveMessageResult receiveMessage(final String queueUrl, final Integer maxNumberOfMessages, final Integer waitTimeSeconds) {
    final ReceiveMessageResult result = sqsClient.receiveMessage(
        new ReceiveMessageRequest(queueUrl)
          .withMaxNumberOfMessages(maxNumberOfMessages)
          .withWaitTimeSeconds(waitTimeSeconds)
          .withAttributeNames(QueueAttributeName.All)
          .withMessageAttributeNames("All"));

    result.getMessages().forEach(message -> sqsClient.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle())));

    return result;
  }

  @SneakyThrows
  private void countDownLatch(final Integer count, final Consumer<CountDownLatch> consumer) {
    final CountDownLatch countDownLatch = new CountDownLatch(count);

    consumer.accept(countDownLatch);

    countDownLatch.await(1L, TimeUnit.MINUTES);
  }

  @Test
  void testSendSingleMessage() {
    final String messageBody = "hello-sqs-" + UUID.randomUUID();

    countDownLatch(1, countDownLatch -> {

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

      final String id = UUID.randomUUID().toString();

      final ListenableFuture<ResponseSuccessEntry,ResponseFailEntry> future = template.send(RequestEntry.builder()
        .withId(id)
        .withValue(messageBody)
        .build());

      template.await().thenRun(template::shutdown).join();

      future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), is(id));
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      });
    });

    final ReceiveMessageResult result = receiveMessage(standardQueueUrl, 1, 5);

    assertThat(result.getMessages(), hasSize(1));

    final Message message = result.getMessages().get(0);
    assertThat(message.getBody(), is(messageBody));
    assertThat(message.getMessageAttributes().keySet(), hasSize(0));
  }

  @Test
  void testSendMultipleMessages() {
    final int messageCount = 500;

    countDownLatch(messageCount, countDownLatch -> {
      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 50L, 10, 10);

      IntStream.range(0, messageCount).forEach(i -> {
        futures.add(
          template.send(RequestEntry.builder()
            .withId(UUID.randomUUID().toString())
            .withValue("msg-" + i + "-" + UUID.randomUUID())
            .build())
          );
      });

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < messageCount) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(messageCount));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("msg-"));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendMessagesExceedingBatchSize() {
    final int messageCount = 25;

    countDownLatch(messageCount, countDownLatch -> {
      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 50L, 10, 10);

      IntStream.range(0, messageCount).forEach(i -> {
        futures.add(template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("batch-test-" + i)
          .build()));
      });

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < messageCount) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(messageCount));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("batch-test-"));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendMessagesWithLinger() {
    final int messageCount = 20;

    countDownLatch(messageCount, countDownLatch -> {
      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 200L, 10, 5);

      IntStream.range(0, messageCount).forEach(i -> {
        futures.add(template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("linger-test-" + i)
          .build()));
      });

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < messageCount) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(messageCount));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("linger-test-"));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendMessageWithgetMessageAttributes() {
    final String messageBody = "attr-test-" + UUID.randomUUID();

    countDownLatch(1, countDownLatch -> {
      final Map<String, Object> messageHeaders = new HashMap<>();
      messageHeaders.put("string-attr", "hello");
      messageHeaders.put("number-attr", 42);

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future = template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .withMessageHeaders(messageHeaders)
        .build());

      template.await().thenRun(template::shutdown).join();

      future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      });
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < 1) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(1));

    messages.forEach(message -> {
      assertThat(message.getBody(), is(messageBody));
      assertThat(message.getMessageAttributes().get("string-attr").getStringValue(), is("hello"));
      assertThat(message.getMessageAttributes().get("number-attr").getStringValue(), is("42"));
    });
  }

  @Test
  void testSendLargeMessage() {
    final String largeBody = RandomStringUtils.secure().nextAlphabetic(262_144);

    countDownLatch(1, countDownLatch -> {
      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 200L, 5, 5);

      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future = template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(largeBody)
        .build());

      template.await().thenRun(template::shutdown).join();

      future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      });
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < 1) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(1));

    messages.forEach(message -> {
      assertThat(message.getBody(), is(largeBody));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendMessageExceedingMaxSize() {
    countDownLatch(1, countDownLatch -> {
      final String oversizedBody = RandomStringUtils.secure().nextAlphabetic((1024 * 1024) + 1);

      final RequestEntry<Object> entry = RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(oversizedBody)
        .build();

      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future = template.send(entry);

      template.await().thenRun(template::shutdown).join();

      future.addCallback(null, failureResult -> {
        assertThat(failureResult.getCode(), is("000"));
        assertThat(failureResult.getId(), is(entry.getId()));
        assertThat(failureResult.getMessage(), containsString("The maximum allowed message size exceeding 1024KB (1,048,576 bytes)."));
        assertThat(failureResult.getSenderFault(), is(true));
        countDownLatch.countDown();
      });
    });

    final List<Message> messages = receiveMessage(standardQueueUrl, 10, 5).getMessages();

    assertThat(messages, hasSize(0));
  }

  @Test
  void testShutdownDrainsPendingMessages() {
    final int messageCount = 5;

    countDownLatch(messageCount, countDownLatch -> {
      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 10_000L, 10, 5);

      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      IntStream.range(0, messageCount).forEach(i -> {
        futures.add(template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("drain-test-" + i)
          .build()));
      });

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < messageCount) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(messageCount));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("drain-test-"));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testTemplateLifecycle() {
    countDownLatch(1, countDownLatch -> {
      final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future = template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue("lifecycle-" + UUID.randomUUID())
        .build());

      template.await().thenRun(template::shutdown).join();

      future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        countDownLatch.countDown();
      });
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < 1) {
      messages.addAll(receiveMessage(standardQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(1));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("lifecycle-"));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendSingleFifoMessage() {
    final String messageBody = "fifo-single-" + UUID.randomUUID();
    final String id = UUID.randomUUID().toString();
    final String groupId = id;

    countDownLatch(1, countDownLatch -> {
      final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 100L, 10, 1);

      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future = template.send(RequestEntry.builder()
        .withId(id)
        .withValue(messageBody)
        .withGroupId(groupId)
        .build());

      template.await().thenRun(template::shutdown).join();

      future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), is(id));
        assertThat(result.getMessageId(), notNullValue());
        assertThat(result.getSequenceNumber(), notNullValue());
        countDownLatch.countDown();
      });
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < 1) {
      messages.addAll(receiveMessage(fifoQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(1));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("fifo-single-"));
      assertThat(message.getAttributes().get(MessageSystemAttributeName.MessageGroupId.toString()), is(groupId));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendFifoMessagesWithOrdering() {
    final int messageCount = 100;
    final String groupId = UUID.randomUUID().toString();

    countDownLatch(1, countDownLatch -> {
      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 50L, 10, 1);

      IntStream.range(0, messageCount).forEach(i -> {
        futures.add(template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("ordered-" + i)
          .withGroupId(groupId)
          .build()));
      });

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        assertThat(result.getSequenceNumber(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < messageCount) {
      messages.addAll(receiveMessage(fifoQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(messageCount));

    messages.forEach(message -> {
      assertThat(message.getBody(), containsString("ordered-"));
      assertThat(message.getAttributes().get(MessageSystemAttributeName.MessageGroupId.toString()), is(groupId));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

  @Test
  void testSendFifoMessageWithDeduplication() {
    final String deduplicationId = UUID.randomUUID().toString();
    final String groupId = UUID.randomUUID().toString();
    final String messageBody = "dedup-test-" + UUID.randomUUID();

    countDownLatch(1, countDownLatch -> {
      final List<ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> futures = new ArrayList<>();

      final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 100L, 10, 1);

      futures.add(template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .withGroupId(groupId)
        .withDeduplicationId(deduplicationId)
        .build()));

      futures.add(template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody + "-duplicate")
        .withGroupId(groupId)
        .withDeduplicationId(deduplicationId)
        .build()));

      template.await().thenRun(template::shutdown).join();

      futures.forEach(future -> future.addCallback(result -> {
        assertThat(result, notNullValue());
        assertThat(result.getId(), notNullValue());
        assertThat(result.getMessageId(), notNullValue());
        assertThat(result.getSequenceNumber(), notNullValue());
        countDownLatch.countDown();
      }));
    });

    final List<Message> messages = new LinkedList<>();

    while (messages.size() < 1) {
      messages.addAll(receiveMessage(fifoQueueUrl, 10, 5).getMessages());
    }

    assertThat(messages, hasSize(1));

    messages.forEach(message -> {
      assertThat(message.getBody(), is(messageBody));
      assertThat(message.getAttributes().get(MessageSystemAttributeName.MessageGroupId.toString()), is(groupId));
      assertThat(message.getAttributes().get(MessageSystemAttributeName.MessageDeduplicationId.toString()), is(deduplicationId));
      assertThat(message.getMessageAttributes().keySet(), hasSize(0));
    });
  }

}
// @formatter:on
