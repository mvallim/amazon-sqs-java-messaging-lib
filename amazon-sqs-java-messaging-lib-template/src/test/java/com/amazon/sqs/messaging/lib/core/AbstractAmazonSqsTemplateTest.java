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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazon.sqs.messaging.lib.concurrent.AmazonSqsThreadPoolExecutor;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

@ExtendWith(MockitoExtension.class)
class AbstractAmazonSqsTemplateTest {

  @Mock
  private AbstractAmazonSqsProducer<String> amazonSnsProducer;

  @Mock
  private AbstractAmazonSqsConsumer<Object, Object, Object, String> amazonSnsConsumer;

  @Mock
  private RequestEntry<String> entry;

  @Mock
  private ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> future;

  private AbstractAmazonSqsTemplate<Object, Object, Object, String> template;

  @BeforeEach
  void setUp() {
    template = new AbstractAmazonSqsTemplate<Object, Object, Object, String>(amazonSnsProducer, amazonSnsConsumer) {
    };
  }

  @Test
  void testSendDelegatesRequestToProducer() {
    when(amazonSnsProducer.send(entry)).thenReturn(future);

    template.send(entry);

    verify(amazonSnsProducer).send(entry);
  }

  @Test
  void testSendReturnsProducerFuture() {
    when(amazonSnsProducer.send(entry)).thenReturn(future);

    final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> result = template.send(entry);

    assertThat(result, is(future));
  }

  @Test
  void testShutdownDelegatesShutdownToProducer() {
    template.shutdown();

    verify(amazonSnsProducer).shutdown();
  }

  @Test
  void testShutdownDelegatesShutdownToConsumer() {
    template.shutdown();

    verify(amazonSnsConsumer).shutdown();
  }

  @Test
  void testAwaitDelegatesAwaitToConsumer() {
    final CompletableFuture<Void> expected = new CompletableFuture<>();
    when(amazonSnsConsumer.await()).thenReturn(expected);

    template.await();

    verify(amazonSnsConsumer).await();
  }

  @Test
  void testAwaitReturnsConsumerCompletableFuture() {
    final CompletableFuture<Void> expected = new CompletableFuture<>();
    when(amazonSnsConsumer.await()).thenReturn(expected);

    final CompletableFuture<Void> result = template.await();

    assertThat(result, is(expected));
  }

  @Test
  void testGetAmazonSnsThreadPoolExecutorReturnsSingleThreadForFifoqueue() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    when(queueProperty.isFifo()).thenReturn(true);

    final AmazonSqsThreadPoolExecutor executor = AbstractAmazonSqsTemplate.getAmazonSqsThreadPoolExecutor(queueProperty);

    assertThat(executor, is(notNullValue()));
    assertThat(executor.getMaximumPoolSize(), is(1));
  }

  @Test
  void testGetAmazonSnsThreadPoolExecutorReturnsConfiguredPoolSizeForStandardqueue() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    when(queueProperty.isFifo()).thenReturn(false);
    when(queueProperty.getMaximumPoolSize()).thenReturn(4);

    final AmazonSqsThreadPoolExecutor executor = AbstractAmazonSqsTemplate.getAmazonSqsThreadPoolExecutor(queueProperty);

    assertThat(executor, is(notNullValue()));
    assertThat(executor.getMaximumPoolSize(), is(4));
  }

  @Test
  void testGetAmazonSnsThreadPoolExecutorReturnsCorrectType() {
    final QueueProperty queueProperty = mock(QueueProperty.class);
    when(queueProperty.isFifo()).thenReturn(false);
    when(queueProperty.getMaximumPoolSize()).thenReturn(2);

    final AmazonSqsThreadPoolExecutor executor = AbstractAmazonSqsTemplate.getAmazonSqsThreadPoolExecutor(queueProperty);

    assertThat(executor, instanceOf(AmazonSqsThreadPoolExecutor.class));
  }

}