/*
 * Copyright 2024 the original author or authors.
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

package com.amazon.sqs.messaging.lib.core.jmh;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.amazon.sqs.messaging.lib.core.AmazonSqsTemplate;
import com.amazon.sqs.messaging.lib.model.QueueProperty;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;

// @formatter:off
@Fork(5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class AmazonSqsTemplateBenchmark {

  private AmazonSqsTemplate<Integer> amazonSnsTemplate;

  @Setup
  public void setup() {
    final AmazonSQS amazonSNS = mock(AmazonSQS.class);

    when(amazonSNS.sendMessageBatch(any())).thenAnswer(invocation -> {
      final SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
      final List<SendMessageBatchResultEntry> resultEntries = request.getEntries().stream()
        .map(entry -> new SendMessageBatchResultEntry().withId(entry.getId()))
        .collect(Collectors.toList());
      return new SendMessageBatchResult().withSuccessful(resultEntries);
    });

    final QueueProperty topicProperty = QueueProperty.builder()
      .fifo(false)
      .linger(70)
      .maxBatchSize(10)
      .maximumPoolSize(512)
      .queueUrl("http://localhost/000000000000/queue")
      .build();

    amazonSnsTemplate = new AmazonSqsTemplate<>(amazonSNS, topicProperty);
  }

  @TearDown
  public void tearDown() {
    amazonSnsTemplate.await().join();
    amazonSnsTemplate.shutdown();
  }

  @Benchmark
  @OperationsPerInvocation(1)
  public void testSend_1() throws InterruptedException {
    amazonSnsTemplate.send(RequestEntry.<Integer>builder().withValue(1).build());
  }

  @Benchmark
  @OperationsPerInvocation(10)
  public void testSend_10() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      amazonSnsTemplate.send(RequestEntry.<Integer>builder().withValue(i).build());
    }
  }

  @Benchmark
  @OperationsPerInvocation(100)
  public void testSend_100() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      amazonSnsTemplate.send(RequestEntry.<Integer>builder().withValue(i).build());
    }
  }

  @Benchmark
  @OperationsPerInvocation(1000)
  public void testSend_1000() throws InterruptedException {
    for (int i = 0; i < 1000; i++) {
      amazonSnsTemplate.send(RequestEntry.<Integer>builder().withValue(i).build());
    }
  }

  @Benchmark
  @OperationsPerInvocation(10000)
  public void testSend_10000() throws InterruptedException {
    for (int i = 0; i < 10000; i++) {
      amazonSnsTemplate.send(RequestEntry.<Integer>builder().withValue(i).build());
    }
  }

}
// @formatter:on
