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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazon.sqs.messaging.lib.metrics.BlockingQueueMetricsDecorator;
import com.amazon.sqs.messaging.lib.metrics.ExecutorServiceMetricsDecorator;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

// @formatter:off
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({ "rawtypes", "unchecked"})
class AbstractAmazonSqsTemplateTest {

  @Mock
  private AbstractAmazonSqsProducer<String> producerMock;

  @Mock
  private AbstractAmazonSqsConsumer<Object, Object, Object, String> consumerMock;

  private AbstractAmazonSqsTemplate<Object, Object, String> template;

  @BeforeEach
  void setUp() {
    template = new AbstractAmazonSqsTemplate<Object, Object, String>(producerMock, consumerMock) { };
  }

  @Test
  void testSendDelegatesToProducer() {
    final RequestEntry<String> requestEntry = RequestEntry.<String>builder().build();
    final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> expectedFuture = new ListenableFutureImpl();
    when(producerMock.send(requestEntry)).thenReturn(expectedFuture);

    final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> result = template.send(requestEntry);

    assertThat(result, is(equalTo(expectedFuture)));
    verify(producerMock).send(requestEntry);
  }

  @Test
  void testShutdownDelegatesToProducer() {
    template.shutdown();
    verify(producerMock).shutdown();
  }

  @Test
  void testShutdownDelegatesToConsumer() {
    template.shutdown();
    verify(consumerMock).shutdown();
  }

  @Test
  void testAwaitDelegatesToConsumer() {
    final CompletableFuture<Void> expectedFuture = CompletableFuture.completedFuture(null);
    when(consumerMock.await()).thenReturn(expectedFuture);

    final CompletableFuture<Void> result = template.await();

    assertThat(result, is(equalTo(expectedFuture)));
    verify(consumerMock).await();
  }

  @Test
  void testGetExecutorServiceReturnsSingleThreadPoolForFifoTopic() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(true)
      .queueUrl("http://localhost/000000000000/queue.fifo")
      .maximumPoolSize(10)
      .maxBatchSize(10)
      .build();

    final ExecutorService executorService = AbstractAmazonSqsTemplate.getExecutorService(queueProperty, new SimpleMeterRegistry());

    assertThat(executorService, is(notNullValue()));
    assertThat(executorService, is(instanceOf(ExecutorServiceMetricsDecorator.class)));
    executorService.shutdownNow();
  }

  @Test
  void testGetExecutorServiceReturnsMultiThreadPoolForNonFifoTopic() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(5)
      .maxBatchSize(10)
      .build();

    final ExecutorService executorService = AbstractAmazonSqsTemplate.getExecutorService(queueProperty, new SimpleMeterRegistry());

    assertThat(executorService, is(notNullValue()));
    assertThat(executorService, is(instanceOf(ExecutorServiceMetricsDecorator.class)));
    executorService.shutdownNow();
  }

  @Test
  void testGetExecutorServiceWithNullMeterRegistryDoesNotThrow() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(4)
      .maxBatchSize(10)
      .build();

    final ExecutorService executorService = AbstractAmazonSqsTemplate.getExecutorService(queueProperty, null);

    assertThat(executorService, is(notNullValue()));
    executorService.shutdownNow();
  }

  @Test
  void testBuilderThrowsWhenAmazonSnsClientIsNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    assertThrows(NullPointerException.class, () -> new AbstractAmazonSqsTemplate.Builder<>(builder -> null, null, queueProperty));
  }

  @Test
  void testBuilderThrowsWhenQueuePropertyIsNull() {
    assertThrows(NullPointerException.class, () -> new AbstractAmazonSqsTemplate.Builder<>(builder -> null, new Object(), null));
  }

  @Test
  void testBuilderThrowsWhenConstructorIsNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    assertThrows(NullPointerException.class, () -> new AbstractAmazonSqsTemplate.Builder<>(null, new Object(), queueProperty));
  }

  @Test
  void testBuilderPendingRequestsThrowsWhenNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThrows(NullPointerException.class, () -> builder.pendingRequests(null));
  }

  @Test
  void testBuilderTopicRequestsThrowsWhenNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThrows(NullPointerException.class, () -> builder.queueRequests(null));
  }

  @Test
  void testBuilderObjectMapperThrowsWhenNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThrows(NullPointerException.class, () -> builder.objectMapper(null));
  }

  @Test
  void testBuilderPublishDecoratorThrowsWhenNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThrows(NullPointerException.class, () -> builder.publishDecorator(null));
  }

  @Test
  void testBuilderMeterRegistryThrowsWhenNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThrows(NullPointerException.class, () -> builder.meterRegistry(null));
  }

  @Test
  void testBuilderStoresAmazonSnsClient() {
    final Object client = new Object();
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, client, queueProperty);

    assertThat(builder.getAmazonSqsClient(), is(equalTo(client)));
  }

  @Test
  void testBuilderStoresQueueProperty() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThat(builder.getQueueProperty(), is(equalTo(queueProperty)));
  }

  @Test
  void testBuilderDefaultPendingRequestsIsConcurrentHashMap() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThat(builder.getPendingRequests(), is(instanceOf(ConcurrentHashMap.class)));
  }

  @Test
  void testBuilderDefaultObjectMapperIsNotNull() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThat(builder.getObjectMapper(), is(notNullValue()));
    assertThat(builder.getObjectMapper(), is(instanceOf(ObjectMapper.class)));
  }

  @Test
  void testBuilderDefaultMeterRegistryIsSimpleMeterRegistry() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    assertThat(builder.getMeterRegistry(), is(instanceOf(SimpleMeterRegistry.class)));
  }

  @Test
  void testBuilderPendingRequestsReturnsSelf() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);
    final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> map = new ConcurrentHashMap<>();

    final AbstractAmazonSqsTemplate.Builder<?, ?, ?, ?, ?> result = builder.pendingRequests(map);

    assertThat(result, is(equalTo(builder)));
  }

  @Test
  void testBuilderTopicRequestsReturnsSelf() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);
    final BlockingQueue<RequestEntry<String>> queue = new LinkedBlockingDeque<>();

    final AbstractAmazonSqsTemplate.Builder<?, ?, ?, ?, ?> result = builder.queueRequests(queue);

    assertThat(result, is(equalTo(builder)));
  }

  @Test
  void testBuilderObjectMapperReturnsSelf() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    final AbstractAmazonSqsTemplate.Builder<?, ?, ?, ?, ?> result = builder.objectMapper(new ObjectMapper());

    assertThat(result, is(equalTo(builder)));
  }

  @Test
  void testBuilderPublishDecoratorReturnsSelf() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    final AbstractAmazonSqsTemplate.Builder<?, ?, ?, ?, ?> result = builder.publishDecorator(UnaryOperator.identity());

    assertThat(result, is(equalTo(builder)));
  }

  @Test
  void testBuilderMeterRegistryReturnsSelf() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);

    final AbstractAmazonSqsTemplate.Builder<?, ?, ?, ?, ?> result = builder.meterRegistry(new SimpleMeterRegistry());

    assertThat(result, is(equalTo(builder)));
  }

  @Test
  void testBuilderBuildWrapsTopicRequestsWithMetricsDecorator() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(4)
      .maxBatchSize(10)
      .build();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> {
      assertThat(b.getQueueRequests(), is(instanceOf(BlockingQueueMetricsDecorator.class)));
      return null;
    }, new Object(), queueProperty);

    builder.build();
  }

  @Test
  void testBuilderBuildCreatesDefaultTopicRequestsWhenNotProvided() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(4)
      .maxBatchSize(10)
      .build();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> {
      assertThat(b.getQueueRequests(), is(notNullValue()));
      return null;
    }, new Object(), queueProperty);

    builder.build();
  }

  @Test
  void testBuilderBuildUsesProvidedTopicRequests() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(4)
      .maxBatchSize(10)
      .build();

    final BlockingQueue<RequestEntry<String>> customQueue = new LinkedBlockingDeque<>();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> {
      assertThat(b.getQueueRequests(), is(instanceOf(BlockingQueueMetricsDecorator.class)));
      return null;
    }, new Object(), queueProperty);

    builder.queueRequests(customQueue).build();
  }

  @Test
  void testBuilderBuildInvokesConstructorFunction() {
    final QueueProperty queueProperty = QueueProperty.builder()
      .fifo(false)
      .queueUrl("http://localhost/000000000000/queue")
      .maximumPoolSize(4)
      .maxBatchSize(10)
      .build();

    final AbstractAmazonSqsTemplate sentinel = new AbstractAmazonSqsTemplate(producerMock, consumerMock) { };

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> sentinel, new Object(), queueProperty);

    final Object result = builder.build();

    assertThat(result, is(equalTo(sentinel)));
  }

  @Test
  void testBuilderSetsPendingRequests() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> customMap = new ConcurrentHashMap<>();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);
    builder.pendingRequests(customMap);

    assertThat(builder.getPendingRequests(), is(equalTo(customMap)));
  }

  @Test
  void testBuilderSetsObjectMapper() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final ObjectMapper customMapper = new ObjectMapper();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);
    builder.objectMapper(customMapper);

    assertThat(builder.getObjectMapper(), is(equalTo(customMapper)));
  }

  @Test
  void testBuilderSetsMeterRegistry() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    final MeterRegistry customRegistry = new SimpleMeterRegistry();

    final AbstractAmazonSqsTemplate.Builder<Object, Object, Object, String, ?> builder = new AbstractAmazonSqsTemplate.Builder<>(b -> null, new Object(), queueProperty);
    builder.meterRegistry(customRegistry);

    assertThat(builder.getMeterRegistry(), is(equalTo(customRegistry)));
  }
}
