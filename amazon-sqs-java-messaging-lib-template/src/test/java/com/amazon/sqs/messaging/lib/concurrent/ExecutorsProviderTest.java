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

package com.amazon.sqs.messaging.lib.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// @formatter:off
class ExecutorsProviderTest {

  private String originalJavaVersion;

  private Method getJavaVersionMethod;

  @BeforeEach
  void setUp() throws Exception {
    originalJavaVersion = System.getProperty("java.version");
    getJavaVersionMethod = ExecutorsProvider.class.getDeclaredMethod("getJavaVersion");
    getJavaVersionMethod.setAccessible(true);
  }

  @AfterEach
  void tearDown() {
    System.setProperty("java.version", originalJavaVersion);
  }

  @Test
  void testGetExecutorServiceReturnsNonNull() {
    final ExecutorService executor = ExecutorsProvider.getExecutorService();
    assertThat(executor, is(notNullValue()));
    executor.shutdown();
  }

  @Test
  void testGetExecutorServiceReturnsRunningExecutor() {
    final ExecutorService executor = ExecutorsProvider.getExecutorService();
    assertThat(executor.isShutdown(), is(false));
    executor.shutdown();
  }

  @Test
  void testGetExecutorServiceExecutesTask() throws Exception {
    final ExecutorService executor = ExecutorsProvider.getExecutorService();
    final AtomicBoolean executed = new AtomicBoolean(false);

    executor.execute(() -> executed.set(true));
    executor.shutdown();

    while (!executor.isTerminated()) {
      Thread.sleep(10);
    }

    assertThat(executed.get(), is(true));
  }

  @Test
  void testGetJavaVersionWithJava8Format() throws Exception {
    final int version = invokeGetJavaVersion("1.8.0_202");
    assertThat(version, is(8));
  }

  @Test
  void testGetJavaVersionWithJava11Format() throws Exception {
    final int version = invokeGetJavaVersion("11.0.1");
    assertThat(version, is(11));
  }

  @Test
  void testGetJavaVersionWithJava17Format() throws Exception {
    final int version = invokeGetJavaVersion("17.0.1");
    assertThat(version, is(17));
  }

  @Test
  void testGetJavaVersionWithJava21Format() throws Exception {
    final int version = invokeGetJavaVersion("21.0.1");
    assertThat(version, is(21));
  }

  @Test
  void testGetJavaVersionWithJava21EaFormat() throws Exception {
    final int version = invokeGetJavaVersion("21-ea");
    assertThat(version, is(21));
  }

  @Test
  void testGetJavaVersionWithJava21NoSeparator() throws Exception {
    final int version = invokeGetJavaVersion("21");
    assertThat(version, is(21));
  }

  @Test
  void testGetJavaVersionWithJava9NoSeparator() throws Exception {
    final int version = invokeGetJavaVersion("9");
    assertThat(version, is(9));
  }

  @Test
  void testGetVirtualExecutorServiceReturnsVirtualThreadExecutor() throws Exception {
    final Method method = ExecutorsProvider.class.getDeclaredMethod("getVirtualExecutorService");
    method.setAccessible(true);

    final ExecutorService executor = (ExecutorService) method.invoke(null);
    assertThat(executor, is(notNullValue()));
    executor.shutdown();
  }

  @Test
  void testGetDefaultExecutorServiceReturnsSingleThreadExecutor() throws Exception {
    final Method method = ExecutorsProvider.class.getDeclaredMethod("getDefaultExecutorService");
    method.setAccessible(true);

    final ExecutorService executor = (ExecutorService) method.invoke(null);
    assertThat(executor, is(notNullValue()));
    executor.shutdown();
  }

  @Test
  void testPrivateConstructorIsNotAccessible() throws Exception {
    final Constructor<ExecutorsProvider> constructor = ExecutorsProvider.class.getDeclaredConstructor();
    assertThat(constructor.canAccess(null), is(false));
  }

  @Test
  void testVirtualExecutorReflectionMethodName() throws Exception {
    final Method ofVirtualMethod = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
    assertThat(ofVirtualMethod, is(notNullValue()));
  }

  @Test
  void testVirtualExecutorReflectionReturnsExecutorService() throws Exception {
    final Method ofVirtualMethod = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
    final Object result = ofVirtualMethod.invoke(null);
    assertThat(result, is(instanceOf(ExecutorService.class)));
    ((ExecutorService) result).shutdown();
  }

  @Test
  void testGetServiceReturnsFunctioningExecutorWithMultipleTasks() throws Exception {
    final ExecutorService executor = ExecutorsProvider.getExecutorService();
    final AtomicBoolean task1 = new AtomicBoolean(false);
    final AtomicBoolean task2 = new AtomicBoolean(false);
    final AtomicBoolean task3 = new AtomicBoolean(false);

    executor.execute(() -> task1.set(true));
    executor.execute(() -> task2.set(true));
    executor.execute(() -> task3.set(true));
    executor.shutdown();

    while (!executor.isTerminated()) {
      Thread.sleep(10);
    }

    assertThat(task1.get(), is(true));
    assertThat(task2.get(), is(true));
    assertThat(task3.get(), is(true));
  }

  private int invokeGetJavaVersion(final String version) throws Exception {
    final Method setter = System.class.getDeclaredMethod("setProperty", String.class, String.class);
    setter.setAccessible(true);
    setter.invoke(null, "java.version", version);

    return (int) getJavaVersionMethod.invoke(null);
  }

}
// @formatter:on
