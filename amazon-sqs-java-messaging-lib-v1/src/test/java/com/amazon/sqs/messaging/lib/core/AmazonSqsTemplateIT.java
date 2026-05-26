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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

// @formatter:off
@Testcontainers
class AmazonSqsTemplateIT {

  @Container
  static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4.0"))
    .withEnv("LOCALSTACK_HOST", "localhost:4566")
    .withEnv("SQS_ENDPOINT_STRATEGY", "dynamic")
    .withReuse(true)
    .withServices(Service.SQS);

  private static AmazonSQS amazonSQS;

  private static String standardQueueUrl;

  private static String fifoQueueUrl;

  @BeforeAll
  static void setup() {
    amazonSQS = AmazonSQSClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(localstack.getEndpoint().toString(), localstack.getRegion()))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
      .build();

    standardQueueUrl = amazonSQS.createQueue(
      new CreateQueueRequest().withQueueName("integration-test-standard-" + UUID.randomUUID())
    ).getQueueUrl();

    final Map<String, String> fifoAttributes = new HashMap<>();
    fifoAttributes.put("FifoQueue", "true");
    fifoAttributes.put("ContentBasedDeduplication", "true");

    fifoQueueUrl = amazonSQS.createQueue(
      new CreateQueueRequest()
        .withQueueName("integration-test-fifo-" + UUID.randomUUID() + ".fifo")
        .withAttributes(fifoAttributes)
    ).getQueueUrl();
  }

  @AfterAll
  static void teardown() {
    if (amazonSQS != null) {
      try {
        amazonSQS.deleteQueue(standardQueueUrl);
      } catch (final Exception e) {
        // ignore
      }
      try {
        amazonSQS.deleteQueue(fifoQueueUrl);
      } catch (final Exception e) {
        // ignore
      }
    }
  }

  @BeforeEach
  void before() {
    purgeQueue(standardQueueUrl);
    purgeQueue(fifoQueueUrl);
  }

  @Test
  void testSendSingleMessage() {
    final String messageBody = "hello-sqs-" + UUID.randomUUID();

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

    try {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .build());

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<Message> messages = receiveMessages(standardQueueUrl, 1, 5);
    assertThat(messages, hasSize(1));
    assertThat(messages.get(0).getBody(), is(messageBody));
  }

  @Test
  void testSendMultipleMessages() {
    final int messageCount = 500;
    final List<String> messageBodies = IntStream.range(0, messageCount)
      .mapToObj(i -> "msg-" + i + "-" + UUID.randomUUID())
      .collect(Collectors.toList());

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 50L, 10, 10);

    try {
      messageBodies.forEach(body -> {
        template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue(body)
          .build());
      });

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<String> receivedBodies = drainQueue(standardQueueUrl, messageCount);
    assertThat(receivedBodies, hasSize(messageCount));
    assertThat(receivedBodies.containsAll(messageBodies), is(true));
  }

  @Test
  void testSendMessagesExceedingBatchSize() {
    final int messageCount = 25;
    final int maxBatchSize = 10;

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 50L, maxBatchSize, 10);

    try {
      IntStream.range(0, messageCount).forEach(i -> {
        template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("batch-test-" + i)
          .build());
      });

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<String> receivedBodies = drainQueue(standardQueueUrl, messageCount);
    assertThat(receivedBodies, hasSize(messageCount));
  }

  @Test
  void testSendMessagesWithLinger() {
    final int messageCount = 20;
    final long linger = 200L;

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, linger, 10, 5);

    try {
      IntStream.range(0, messageCount).forEach(i -> {
        template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("linger-test-" + i)
          .build());
      });

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<String> receivedBodies = drainQueue(standardQueueUrl, messageCount);
    assertThat(receivedBodies, hasSize(messageCount));
  }

  @Test
  void testSendMessageWithMessageAttributes() {
    final String messageBody = "attr-test-" + UUID.randomUUID();

    final Map<String, MessageAttributeValue> expectedAttributes = new HashMap<>();
    expectedAttributes.put("string-attr", new MessageAttributeValue().withDataType("String").withStringValue("hello"));
    expectedAttributes.put("number-attr", new MessageAttributeValue().withDataType("Number").withStringValue("42"));

    final Map<String, Object> messageHeaders = new HashMap<>();
    messageHeaders.put("string-attr", "hello");
    messageHeaders.put("number-attr", 42);

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

    try {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .withMessageHeaders(messageHeaders)
        .build());

      template.await().join();
    } finally {
      template.shutdown();
    }

    final ReceiveMessageResult result = amazonSQS.receiveMessage(
      new ReceiveMessageRequest()
        .withQueueUrl(standardQueueUrl)
        .withMaxNumberOfMessages(1)
        .withWaitTimeSeconds(5)
        .withMessageAttributeNames("All"));

    assertThat(result.getMessages(), hasSize(1));

    final Message message = result.getMessages().get(0);
    assertThat(message.getBody(), is(messageBody));
    assertThat(message.getMessageAttributes().get("string-attr").getStringValue(), is("hello"));
    assertThat(message.getMessageAttributes().get("number-attr").getStringValue(), is("42"));
  }

  @Test
  void testSendLargeMessage() {
    final String largeBody = RandomStringUtils.secure().nextAlphabetic(262_144);

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 200L, 5, 5);

    try {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(largeBody)
        .build());

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<Message> messages = receiveMessages(standardQueueUrl, 1, 10);
    assertThat(messages, hasSize(1));
    assertThat(messages.get(0).getBody(), is(largeBody));
  }

  @Test
  void testSendMessageExceedingMaxSize() {
    final String oversizedBody = RandomStringUtils.secure().nextAlphabetic((1024 * 1024) + 1);

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

    final RequestEntry<Object> entry = RequestEntry.builder()
      .withId(UUID.randomUUID().toString())
      .withValue(oversizedBody)
      .build();

    final AtomicReference<ResponseFailEntry> responseFailEntry = new AtomicReference<>();

    try {
      template.send(entry).addCallback(null, failCallback -> {
        responseFailEntry.set(failCallback);
      });

      template.await().join();
    } finally {
      template.shutdown();
    }

    assertThat(responseFailEntry.get().getCode(), is("000"));
    assertThat(responseFailEntry.get().getId(), is(entry.getId()));
    assertThat(responseFailEntry.get().getMessage(), containsString("The maximum allowed message size exceeding 1024KB (1,048,576 bytes)."));
    assertThat(responseFailEntry.get().getSenderFault(), is(true));
  }

  @Test
  void testShutdownDrainsPendingMessages() {
    final int messageCount = 5;

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 10_000L, 10, 5);

    IntStream.range(0, messageCount).forEach(i -> {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue("drain-test-" + i)
        .build());
    });

    template.shutdown();

    final List<String> receivedBodies = drainQueue(standardQueueUrl, messageCount);
    assertThat(receivedBodies, hasSize(messageCount));
  }

  @Test
  void testTemplateLifecycle() {
    final String messageBody = "lifecycle-" + UUID.randomUUID();

    final AmazonSqsTemplate<Object> template = createTemplate(standardQueueUrl, false, 100L, 10, 5);

    template.send(RequestEntry.builder()
      .withId(UUID.randomUUID().toString())
      .withValue(messageBody)
      .build());

    final CompletableFuture<Void> awaitFuture = template.await();
    assertThat(awaitFuture, notNullValue());

    awaitFuture.thenRun(template::shutdown).join();

    final List<Message> messages = receiveMessages(standardQueueUrl, 1, 5);
    assertThat(messages, hasSize(1));
    assertThat(messages.get(0).getBody(), is(messageBody));
  }

  @Test
  void testSendSingleFifoMessage() {
    final String messageBody = "fifo-single-" + UUID.randomUUID();
    final String groupId = UUID.randomUUID().toString();

    final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 100L, 10, 1);

    try {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .withGroupId(groupId)
        .build());

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<Message> messages = receiveMessages(fifoQueueUrl, 1, 5);
    assertThat(messages, hasSize(1));
    assertThat(messages.get(0).getBody(), is(messageBody));
    assertThat(messages.get(0).getAttributes().get("MessageGroupId"), is(groupId));
  }

  @Test
  void testSendFifoMessagesWithOrdering() {
    final int messageCount = 100;
    final String groupId = UUID.randomUUID().toString();

    final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 50L, 10, 1);

    try {
      IntStream.range(0, messageCount).forEach(i -> {
        template.send(RequestEntry.builder()
          .withId(UUID.randomUUID().toString())
          .withValue("ordered-" + i)
          .withGroupId(groupId)
          .build());
      });

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<String> receivedBodies = drainQueue(fifoQueueUrl, messageCount);
    assertThat(receivedBodies, hasSize(messageCount));

    for (int i = 0; i < receivedBodies.size(); i++) {
      assertThat(receivedBodies.get(i), is("ordered-" + i));
    }
  }

  @Test
  void testSendFifoMessageWithDeduplication() {
    final String deduplicationId = UUID.randomUUID().toString();
    final String groupId = UUID.randomUUID().toString();
    final String messageBody = "dedup-test-" + UUID.randomUUID();

    final AmazonSqsTemplate<Object> template = createTemplate(fifoQueueUrl, true, 100L, 10, 1);

    try {
      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody)
        .withGroupId(groupId)
        .withDeduplicationId(deduplicationId)
        .build());

      template.send(RequestEntry.builder()
        .withId(UUID.randomUUID().toString())
        .withValue(messageBody + "-duplicate")
        .withGroupId(groupId)
        .withDeduplicationId(deduplicationId)
        .build());

      template.await().join();
    } finally {
      template.shutdown();
    }

    final List<Message> messages = receiveMessages(fifoQueueUrl, 2, 5);
    assertThat(messages, hasSize(1));
    assertThat(messages.get(0).getBody(), is(messageBody));
  }

  private AmazonSqsTemplate<Object> createTemplate(final String queueUrl, final boolean fifo,
      final long linger, final int maxBatchSize, final int maxPoolSize) {

    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(fifo)
      .linger(linger)
      .maxBatchSize(maxBatchSize)
      .maximumPoolSize(maxPoolSize)
      .queueUrl(queueUrl)
      .build();

    return new AmazonSqsTemplate<>(amazonSQS, queueProperty, new RingBufferBlockingQueue<>(1024));
  }

  private List<Message> receiveMessages(final String queueUrl, final int maxNumberOfMessages, final int waitTimeSeconds) {
    return amazonSQS.receiveMessage(
      new ReceiveMessageRequest()
        .withQueueUrl(queueUrl)
        .withMaxNumberOfMessages(maxNumberOfMessages)
        .withWaitTimeSeconds(waitTimeSeconds)
        .withMessageAttributeNames("All")
        .withAttributeNames("All")
    ).getMessages();
  }

  private List<String> drainQueue(final String queueUrl, final int expectedCount) {
    final List<String> allBodies = new LinkedList<>();

    while (allBodies.size() < expectedCount) {
      final ReceiveMessageResult result = amazonSQS.receiveMessage(
        new ReceiveMessageRequest()
          .withQueueUrl(queueUrl)
          .withMaxNumberOfMessages(10)
          .withWaitTimeSeconds(5)
          .withMessageAttributeNames("All")
          .withAttributeNames("All"));

      if (result.getMessages().isEmpty()) {
        break;
      }

      result.getMessages().forEach(msg -> {
        allBodies.add(msg.getBody());
        amazonSQS.deleteMessage(queueUrl, msg.getReceiptHandle());
      });
    }

    return allBodies;
  }

  private void purgeQueue(final String queueUrl) {
    amazonSQS.purgeQueue(new PurgeQueueRequest(queueUrl));
  }

}
// @formatter:on
