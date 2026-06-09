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

package com.amazon.sqs.messaging.lib.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class AmazonSqsThreadPoolExecutorTest {

  @Test
  void testConstructorCreatesInstance() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor, is(notNullValue()));
    executor.shutdownNow();
  }

  @Test
  void testExtendsThreadPoolExecutor() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor, is(instanceOf(ThreadPoolExecutor.class)));
    executor.shutdownNow();
  }

  @Test
  void testCorePoolSizeIsZero() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getCorePoolSize(), is(equalTo(0)));
    executor.shutdownNow();
  }

  @Test
  void testMaximumPoolSizeMatchesConstructorArgument() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(8);
    assertThat(executor.getMaximumPoolSize(), is(equalTo(8)));
    executor.shutdownNow();
  }

  @Test
  void testMaximumPoolSizeOfOne() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(1);
    assertThat(executor.getMaximumPoolSize(), is(equalTo(1)));
    executor.shutdownNow();
  }

  @Test
  void testKeepAliveTimeIsSetTo60Seconds() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getKeepAliveTime(TimeUnit.SECONDS), is(equalTo(60L)));
    executor.shutdownNow();
  }

  @Test
  void testQueueIsSynchronousQueue() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getQueue(), is(instanceOf(SynchronousQueue.class)));
    executor.shutdownNow();
  }

  @Test
  void testRejectedExecutionHandlerIsBlockingSubmissionPolicy() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getRejectedExecutionHandler(), is(instanceOf(BlockingSubmissionPolicy.class)));
    executor.shutdownNow();
  }

  @Test
  void testThreadFactoryIsNotNull() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getThreadFactory(), is(notNullValue()));
    executor.shutdownNow();
  }

  @Test
  void testIsNotShutdownAfterCreation() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.isShutdown(), is(false));
    executor.shutdownNow();
  }

  @Test
  void testIsShutdownAfterShutdownNow() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    executor.shutdownNow();
    assertThat(executor.isShutdown(), is(true));
  }

  @Test
  void testActiveCountIsZeroAfterCreation() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getActiveCount(), is(equalTo(0)));
    executor.shutdownNow();
  }

  @Test
  void testTaskCountIsZeroAfterCreation() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getTaskCount(), is(equalTo(0L)));
    executor.shutdownNow();
  }

  @Test
  void testCompletedTaskCountIsZeroAfterCreation() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getCompletedTaskCount(), is(equalTo(0L)));
    executor.shutdownNow();
  }

  @Test
  void testQueueIsEmptyAfterCreation() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getQueue().isEmpty(), is(true));
    executor.shutdownNow();
  }

  @Test
  void testPoolSizeIsZeroBeforeAnyTask() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getPoolSize(), is(equalTo(0)));
    executor.shutdownNow();
  }

  @Test
  void testLargestPoolSizeIsZeroBeforeAnyTask() {
    final AmazonSqsThreadPoolExecutor executor = new AmazonSqsThreadPoolExecutor(4);
    assertThat(executor.getLargestPoolSize(), is(equalTo(0)));
    executor.shutdownNow();
  }
}