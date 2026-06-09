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

package com.amazon.sqs.messaging.lib.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazon.sqs.messaging.lib.core.AmazonSqsConsumer;
import com.amazon.sqs.messaging.lib.model.QueueProperty;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@ExtendWith(MockitoExtension.class)
class AbstractAmazonSqsConsumerMetricsDecoratorTest {

  private static class TestableDecorator extends AbstractAmazonSqsConsumerMetricsDecorator<Object, Object> {

    TestableDecorator(final AmazonSqsConsumer<Object, Object> delegate, final QueueProperty queueProperty, final MeterRegistry meterRegistry) {
      super(delegate, queueProperty, meterRegistry);
    }

    @Override
    public void handleError(final Object publishBatchRequest, final Throwable throwable) {
      // no-op for testing
    }

    @Override
    public void handleResponse(final Object publishBatchResult) {
      // no-op for testing
    }

    @Override
    public Object publish(final Object publishBatchRequest) {
      return null;
    }

  }

  private static final String QUEUE_URL = "http://localhost/000000000000/queue";

  private TestableDecorator decorator;

  @Mock
  private AmazonSqsConsumer<Object, Object> delegate;

  @Mock
  private QueueProperty queueProperty;

  @Spy
  private SimpleMeterRegistry meterRegistry;

  @BeforeEach
  void setup() {
    when(queueProperty.getQueueUrl()).thenReturn(QUEUE_URL);

    decorator = new TestableDecorator(delegate, queueProperty, meterRegistry);
  }

  @Nested
  class MetricNameConstants {

    @Test
    void testMetricPublishAttemptsHasSnsPrefix() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_ATTEMPTS, containsString("sqs"));
    }

    @Test
    void testMetricPublishAttemptsValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_ATTEMPTS, equalTo("sqs.publish.attempts"));
    }

    @Test
    void testMetricPublishSuccessValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_SUCCESS, equalTo("sqs.publish.success"));
    }

    @Test
    void testMetricPublishFailureValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_FAILURE, equalTo("sqs.publish.failure"));
    }

    @Test
    void testMetricPublishDurationValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_DURATION, equalTo("sqs.publish.duration"));
    }

    @Test
    void testMetricPublishBatchSizeValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_BATCH_SIZE, equalTo("sqs.publish.batch.size"));
    }

    @Test
    void testMetricPublishInflightValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_INFLIGHT, equalTo("sqs.publish.inflight"));
    }

    @Test
    void testTagErrorCodeValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.TAG_ERROR_CODE, equalTo("error_code"));
    }

    @Test
    void testTagErrorTypeValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.TAG_ERROR_TYPE, equalTo("error_type"));
    }

    @Test
    void testErrorTypeAmazonValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.ERROR_TYPE_AMAZON, equalTo("amazon_service_exception"));
    }

    @Test
    void testErrorTypeOtherValue() {
      assertThat(AbstractAmazonSqsConsumerMetricsDecorator.ERROR_TYPE_OTHER, equalTo("unknown"));
    }
  }

  @Nested
  class ConstructorInitialization {

    @Test
    void testDelegateIsSet() {
      assertThat(decorator.delegate, is(sameInstance(delegate)));
    }

    @Test
    void testRegistryIsNotNull() {
      assertThat(decorator.registry, is(notNullValue()));
    }

    @Test
    void testTagsAreNotNull() {
      assertThat(decorator.tags, is(notNullValue()));
    }

    @Test
    void testTagsContainTopicArn() {
      assertThat(decorator.tags.stream().anyMatch(t -> "queue".equals(t.getKey()) && QUEUE_URL.equals(t.getValue())), is(true));
    }

    @Test
    void testPublishAttemptsCounterIsNotNull() {
      assertThat(decorator.publishAttemptsCounter, is(notNullValue()));
    }

    @Test
    void testSuccessCounterIsNotNull() {
      assertThat(decorator.successCounter, is(notNullValue()));
    }

    @Test
    void testPublishTimerIsNotNull() {
      assertThat(decorator.publishTimer, is(notNullValue()));
    }

    @Test
    void testBatchSizeSummaryIsNotNull() {
      assertThat(decorator.batchSizeSummary, is(notNullValue()));
    }

    @Test
    void testInflightGaugeIsNotNull() {
      assertThat(decorator.inflightGauge, is(notNullValue()));
    }

    @Test
    void testInflightGaugeInitialValueIsZero() {
      assertThat(decorator.inflightGauge.get(), equalTo(0));
    }

    @Test
    void testConstructorWithNullMeterRegistryDoesNotThrow() {
      final TestableDecorator nullRegistryDecorator = new TestableDecorator(delegate, queueProperty, null);

      assertThat(nullRegistryDecorator, is(notNullValue()));
    }

    @Test
    void testConstructorWithNullMeterRegistryInitializesCounters() {
      final TestableDecorator nullRegistryDecorator = new TestableDecorator(delegate, queueProperty, null);

      assertThat(nullRegistryDecorator.publishAttemptsCounter, is(notNullValue()));
      assertThat(nullRegistryDecorator.successCounter, is(notNullValue()));
    }
  }

  @Nested
  class MetersRegisteredInRegistry {

    @Test
    void testPublishAttemptsCounterRegisteredInRegistry() {
      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_ATTEMPTS).counter(), is(notNullValue()));
    }

    @Test
    void testPublishSuccessCounterRegisteredInRegistry() {
      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_SUCCESS).counter(), is(notNullValue()));
    }

    @Test
    void testPublishTimerRegisteredInRegistry() {
      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_DURATION).timer(), is(notNullValue()));
    }

    @Test
    void testBatchSizeSummaryRegisteredInRegistry() {
      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_BATCH_SIZE).summary(), is(notNullValue()));
    }

    @Test
    void testInflightGaugeRegisteredInRegistry() {
      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_INFLIGHT).gauge(), is(notNullValue()));
    }

    @Test
    void testPublishAttemptsCounterInitialValueIsZero() {
      assertThat(decorator.publishAttemptsCounter.count(), equalTo(0.0));
    }

    @Test
    void testSuccessCounterInitialValueIsZero() {
      assertThat(decorator.successCounter.count(), equalTo(0.0));
    }

    @Test
    void testPublishAttemptsCounterIncrementsCorrectly() {
      decorator.publishAttemptsCounter.increment();

      assertThat(decorator.publishAttemptsCounter.count(), equalTo(1.0));
    }

    @Test
    void testSuccessCounterIncrementsCorrectly() {
      decorator.successCounter.increment(3);

      assertThat(decorator.successCounter.count(), equalTo(3.0));
    }
  }

  @Nested
  class FailureCounter {

    @Test
    void testFailureCounterReturnsNotNull() {
      final Counter counter = decorator.failureCounter("400", "amazon_service_exception");

      assertThat(counter, is(notNullValue()));
    }

    @Test
    void testFailureCounterInitialValueIsZero() {
      final Counter counter = decorator.failureCounter("500", "unknown");

      assertThat(counter.count(), equalTo(0.0));
    }

    @Test
    void testFailureCounterIncrementsCorrectly() {
      final Counter counter = decorator.failureCounter("400", "amazon_service_exception");
      counter.increment();

      assertThat(counter.count(), equalTo(1.0));
    }

    @Test
    void testFailureCounterRegisteredInRegistry() {
      decorator.failureCounter("InvalidParameter", "amazon_service_exception");

      assertThat(meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_FAILURE).counter(), is(notNullValue()));
    }

    @Test
    void testFailureCounterWithSameTagsReturnsSameMeter() {
      final Counter first = decorator.failureCounter("400", "amazon_service_exception");
      final Counter second = decorator.failureCounter("400", "amazon_service_exception");

      assertThat(first, is(sameInstance(second)));
    }

    @Test
    void testFailureCounterWithDifferentErrorCodesAreDistinct() {
      final Counter counter400 = decorator.failureCounter("400", "amazon_service_exception");
      final Counter counter500 = decorator.failureCounter("500", "amazon_service_exception");

      assertThat(counter400, is(not(sameInstance(counter500))));
    }

    @Test
    void testFailureCounterWithDifferentErrorTypesAreDistinct() {
      final Counter amazon = decorator.failureCounter("400", "amazon_service_exception");
      final Counter unknown = decorator.failureCounter("400", "unknown");

      assertThat(amazon, is(not(sameInstance(unknown))));
    }

    @Test
    void testFailureCounterTagsIncludeTopicArn() {
      decorator.failureCounter("400", "amazon_service_exception");

      final Counter found = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_FAILURE).tag("queue", QUEUE_URL).counter();

      assertThat(found, is(notNullValue()));
    }

    @Test
    void testFailureCounterTagsIncludeErrorCode() {
      decorator.failureCounter("InvalidParam", "amazon_service_exception");

      final Counter found = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_FAILURE).tag(AbstractAmazonSqsConsumerMetricsDecorator.TAG_ERROR_CODE, "InvalidParam").counter();

      assertThat(found, is(notNullValue()));
    }

    @Test
    void testFailureCounterTagsIncludeErrorType() {
      decorator.failureCounter("400", "unknown");

      final Counter found = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_FAILURE).tag(AbstractAmazonSqsConsumerMetricsDecorator.TAG_ERROR_TYPE, "unknown").counter();

      assertThat(found, is(notNullValue()));
    }
  }

  @Nested
  class InflightGauge {

    @Test
    void testInflightGaugeReflectsIncrementInRegistry() {
      decorator.inflightGauge.incrementAndGet();

      final double gaugeValue = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_INFLIGHT).gauge().value();

      assertThat(gaugeValue, equalTo(1.0));
    }

    @Test
    void testInflightGaugeReflectsDecrementInRegistry() {
      decorator.inflightGauge.set(3);
      decorator.inflightGauge.decrementAndGet();

      final double gaugeValue = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_INFLIGHT).gauge().value();

      assertThat(gaugeValue, equalTo(2.0));
    }

    @Test
    void testInflightGaugeReflectsZeroAfterReset() {
      decorator.inflightGauge.set(5);
      decorator.inflightGauge.set(0);

      final double gaugeValue = meterRegistry.find(AbstractAmazonSqsConsumerMetricsDecorator.METRIC_PUBLISH_INFLIGHT).gauge().value();

      assertThat(gaugeValue, equalTo(0.0));
    }
  }

  @Nested
  class Shutdown {

    @Test
    void testShutdownDelegatesToDelegate() {
      decorator.shutdown();

      verify(delegate).shutdown();
    }

    @Test
    void testShutdownCanBeCalledMultipleTimes() {
      decorator.shutdown();
      decorator.shutdown();

      verify(delegate, org.mockito.Mockito.times(2)).shutdown();
    }
  }

  @Nested
  class Await {

    @Test
    void testAwaitDelegatesToDelegate() {
      final CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
      when(delegate.await()).thenReturn(future);

      final CompletableFuture<Void> result = decorator.await();

      assertThat(result, is(sameInstance(future)));
      verify(delegate).await();
    }

    @Test
    void testAwaitReturnsNotNull() {
      when(delegate.await()).thenReturn(CompletableFuture.completedFuture(null));

      assertThat(decorator.await(), is(notNullValue()));
    }

    @Test
    void testAwaitPropagatesDelegateResult() {
      final CompletableFuture<Void> expected = new CompletableFuture<>();
      when(delegate.await()).thenReturn(expected);

      final CompletableFuture<Void> result = decorator.await();

      assertThat(result, is(sameInstance(expected)));
    }
  }

}