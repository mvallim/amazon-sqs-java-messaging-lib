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

package com.amazon.sqs.messaging.lib.concurrent;

import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.amazon.sqs.messaging.lib.concurrent.AmazonSqsThreadPoolExecutor;

// @formatter:off
class AmazonSqsThreadPoolExecutorTest {

  @Test
  void testSuccessCounters() {
    final AmazonSqsThreadPoolExecutor amazonSqsThreadPoolExecutor = new AmazonSqsThreadPoolExecutor(10);

    assertThat(amazonSqsThreadPoolExecutor.getActiveTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getFailedTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getCorePoolSize(), is(0));
  }

  @Test
  void testSuccessSucceededTaskCount() throws InterruptedException {
    final AmazonSqsThreadPoolExecutor amazonSqsThreadPoolExecutor = new AmazonSqsThreadPoolExecutor(10);

    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));

    for(int i = 0; i < 300; i++) {
      amazonSqsThreadPoolExecutor.execute(() -> {
        try {
          Thread.sleep(1);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      });
    }

    amazonSqsThreadPoolExecutor.shutdown();

    if (!amazonSqsThreadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      amazonSqsThreadPoolExecutor.shutdownNow();
    }

    assertThat(amazonSqsThreadPoolExecutor.getActiveTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(300));
    assertThat(amazonSqsThreadPoolExecutor.getFailedTaskCount(), is(0));
  }

  @Test
  void testSuccessFailedTaskCount() throws InterruptedException {
    final AmazonSqsThreadPoolExecutor amazonSqsThreadPoolExecutor = new AmazonSqsThreadPoolExecutor(10);

    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));

    for(int i = 0; i < 300; i++) {
      amazonSqsThreadPoolExecutor.execute(() -> { throw new RuntimeException(); });
    }

    amazonSqsThreadPoolExecutor.shutdown();

    if (!amazonSqsThreadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      amazonSqsThreadPoolExecutor.shutdownNow();
    }

    assertThat(amazonSqsThreadPoolExecutor.getActiveTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getFailedTaskCount(), is(300));
  }

  @Test
  void testSuccessActiveTaskCount() throws InterruptedException {
    final AmazonSqsThreadPoolExecutor amazonSqsThreadPoolExecutor = new AmazonSqsThreadPoolExecutor(10);

    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));

    for(int i = 0; i < 10; i++) {
      amazonSqsThreadPoolExecutor.execute(() -> {
        while(true) {
          try {
            Thread.sleep(1);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }

    amazonSqsThreadPoolExecutor.shutdown();

    if (!amazonSqsThreadPoolExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      amazonSqsThreadPoolExecutor.shutdownNow();
    }

    assertThat(amazonSqsThreadPoolExecutor.getActiveTaskCount(), is(10));
    assertThat(amazonSqsThreadPoolExecutor.getSucceededTaskCount(), is(0));
    assertThat(amazonSqsThreadPoolExecutor.getFailedTaskCount(), is(0));
  }

  @Test
  void testSuccessBlockingSubmissionPolicy() throws InterruptedException {
    final AmazonSqsThreadPoolExecutor amazonSqsThreadPoolExecutor = new AmazonSqsThreadPoolExecutor(1);

    amazonSqsThreadPoolExecutor.execute(() -> {
      while(true) {
        try {
          Thread.sleep(1);
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    catchThrowableOfType(() -> amazonSqsThreadPoolExecutor.execute(() -> { }), RejectedExecutionException.class);
  }

}
// @formatter:on
