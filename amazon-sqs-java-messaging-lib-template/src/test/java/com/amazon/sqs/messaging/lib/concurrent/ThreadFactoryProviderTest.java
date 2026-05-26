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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// @formatter:off
class ThreadFactoryProviderTest {

  private String originalJavaVersion;

  private Method getJavaVersionMethod;

  @BeforeEach
  void setUp() throws Exception {
    originalJavaVersion = System.getProperty("java.version");
    getJavaVersionMethod = ThreadFactoryProvider.class.getDeclaredMethod("getJavaVersion");
    getJavaVersionMethod.setAccessible(true);
  }

  @AfterEach
  void tearDown() {
    System.setProperty("java.version", originalJavaVersion);
  }

  @Test
  void testGetThreadFactoryReturnsNonNull() {
    final ThreadFactory factory = ThreadFactoryProvider.getThreadFactory();
    assertThat(factory, is(notNullValue()));
  }

  @Test
  void testGetThreadFactoryCreatesThread() {
    final ThreadFactory factory = ThreadFactoryProvider.getThreadFactory();
    final Thread thread = factory.newThread(() -> {});
    assertThat(thread, is(notNullValue()));
  }

  @Test
  void testGetThreadFactoryCreatesRunningThread() throws Exception {
    final ThreadFactory factory = ThreadFactoryProvider.getThreadFactory();
    final AtomicBoolean executed = new AtomicBoolean(false);

    final Thread thread = factory.newThread(() -> executed.set(true));
    thread.start();
    thread.join(5000);

    assertThat(executed.get(), is(true));
  }

  @Test
  void testGetThreadFactoryCreatedThreadHasName() {
    final ThreadFactory factory = ThreadFactoryProvider.getThreadFactory();
    final Thread thread = factory.newThread(() -> {});
    assertThat(thread.getName(), is(notNullValue()));
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
  void testGetVirtualThreadFactoryReturnsVirtualFactory() throws Exception {
    final Method method = ThreadFactoryProvider.class.getDeclaredMethod("getVirtualThreadFactory");
    method.setAccessible(true);

    final ThreadFactory factory = (ThreadFactory) method.invoke(null);
    assertThat(factory, is(notNullValue()));
  }

  @Test
  void testGetVirtualThreadFactoryCreatesThread() throws Exception {
    final Method method = ThreadFactoryProvider.class.getDeclaredMethod("getVirtualThreadFactory");
    method.setAccessible(true);

    final ThreadFactory factory = (ThreadFactory) method.invoke(null);
    final Thread thread = factory.newThread(() -> {});
    assertThat(thread, is(notNullValue()));
  }

  @Test
  void testGetDefaultThreadFactoryReturnsDefaultFactory() throws Exception {
    final Method method = ThreadFactoryProvider.class.getDeclaredMethod("getDefaultThreadFactory");
    method.setAccessible(true);

    final ThreadFactory factory = (ThreadFactory) method.invoke(null);
    assertThat(factory, is(notNullValue()));
  }

  @Test
  void testGetDefaultThreadFactoryCreatesThread() throws Exception {
    final Method method = ThreadFactoryProvider.class.getDeclaredMethod("getDefaultThreadFactory");
    method.setAccessible(true);

    final ThreadFactory factory = (ThreadFactory) method.invoke(null);
    final Thread thread = factory.newThread(() -> {});
    assertThat(thread, is(notNullValue()));
  }

  @Test
  void testPrivateConstructorIsNotAccessible() throws Exception {
    final Constructor<ThreadFactoryProvider> constructor = ThreadFactoryProvider.class.getDeclaredConstructor();
    assertThat(constructor.canAccess(null), is(false));
  }

  @Test
  void testOfVirtualMethodExists() throws Exception {
    final Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
    assertThat(ofVirtualMethod, is(notNullValue()));
  }

  @Test
  void testOfVirtualBuilderFactoryMethodExists() throws Exception {
    final Class<?> clazzOfVirtual = Class.forName("java.lang.Thread$Builder$OfVirtual");
    final Method factoryMethod = clazzOfVirtual.getMethod("factory");
    assertThat(factoryMethod, is(notNullValue()));
  }

  @Test
  void testOfVirtualReturnsBuilder() throws Exception {
    final Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
    final Object builder = ofVirtualMethod.invoke(null);
    assertThat(builder, is(notNullValue()));
  }

  @Test
  void testOfVirtualBuilderCreatesFactory() throws Exception {
    final Class<?> clazzOfVirtual = Class.forName("java.lang.Thread$Builder$OfVirtual");
    final Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
    final Method factoryMethod = clazzOfVirtual.getMethod("factory");

    final Object builder = ofVirtualMethod.invoke(null);
    final ThreadFactory factory = (ThreadFactory) factoryMethod.invoke(builder);
    assertThat(factory, is(notNullValue()));
  }

  @Test
  void testVirtualFactoryCreatesVirtualThread() throws Exception {
    final Class<?> clazzOfVirtual = Class.forName("java.lang.Thread$Builder$OfVirtual");
    final Method ofVirtualMethod = Thread.class.getMethod("ofVirtual");
    final Method factoryMethod = clazzOfVirtual.getMethod("factory");

    final Object builder = ofVirtualMethod.invoke(null);
    final ThreadFactory factory = (ThreadFactory) factoryMethod.invoke(builder);
    final Thread thread = factory.newThread(() -> {});

    assertThat(thread.isVirtual(), is(true));
  }

  @Test
  void testGetThreadFactoryCreatesMultipleThreads() throws Exception {
    final ThreadFactory factory = ThreadFactoryProvider.getThreadFactory();
    final AtomicBoolean task1 = new AtomicBoolean(false);
    final AtomicBoolean task2 = new AtomicBoolean(false);
    final AtomicBoolean task3 = new AtomicBoolean(false);

    final Thread t1 = factory.newThread(() -> task1.set(true));
    final Thread t2 = factory.newThread(() -> task2.set(true));
    final Thread t3 = factory.newThread(() -> task3.set(true));

    t1.start(); t2.start(); t3.start();
    t1.join(5000); t2.join(5000); t3.join(5000);

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
