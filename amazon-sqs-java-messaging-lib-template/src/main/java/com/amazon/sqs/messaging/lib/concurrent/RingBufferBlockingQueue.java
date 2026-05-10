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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import lombok.Getter;
import lombok.Locked;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * A lock-based ring buffer implementation of {@link BlockingQueue} with a fixed
 * capacity. Supports blocking {@code put} and {@code take} operations with
 * fairness policy.
 *
 * @param <E> the element type
 */
public class RingBufferBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

  private static final int DEFAULT_CAPACITY = 2048;

  private final AtomicReferenceArray<Entry<E>> buffer;

  private final int capacity;

  private final AtomicLong writeSequence = new AtomicLong(-1);

  private final AtomicLong readSequence = new AtomicLong(0);

  private final AtomicInteger size = new AtomicInteger(0);

  private final ReentrantLock reentrantLock;

  private final Condition waitingConsumer;

  private final Condition waitingProducer;

  /**
   * Creates a ring buffer blocking queue with the specified capacity.
   *
   * @param capacity the fixed capacity of the ring buffer
   */
  public RingBufferBlockingQueue(final int capacity) {
    this.capacity = capacity;
    buffer = new AtomicReferenceArray<>(capacity);
    reentrantLock = new ReentrantLock(true);
    waitingConsumer = reentrantLock.newCondition();
    waitingProducer = reentrantLock.newCondition();
    IntStream.range(0, capacity).forEach(idx -> buffer.set(idx, new Entry<>()));
  }

  /**
   * Creates a ring buffer blocking queue with the default capacity of 2048.
   */
  public RingBufferBlockingQueue() {
    this(RingBufferBlockingQueue.DEFAULT_CAPACITY);
  }

  /**
   * Prevents sequence overflow by wrapping around if the value approaches
   * Long.MAX_VALUE.
   *
   * @param sequence the current sequence value
   * @return the sequence, or a wrapped value if near overflow
   */
  private long avoidSequenceOverflow(final long sequence) {
    return (sequence < Long.MAX_VALUE ? sequence : wrap(sequence));
  }

  /**
   * Computes the buffer index for a given sequence number using modulo
   * arithmetic.
   *
   * @param sequence the sequence number
   * @return the buffer index
   */
  private int wrap(final long sequence) {
    return Math.toIntExact(sequence % capacity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    return size.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEmpty() {
    return size.get() == 0;
  }

  /**
   * Checks if the buffer has reached its capacity.
   *
   * @return true if the buffer is full
   */
  public boolean isFull() {
    return size.get() >= capacity;
  }

  /**
   * Returns the current write sequence number.
   *
   * @return the write sequence
   */
  public long writeSequence() {
    return writeSequence.get();
  }

  /**
   * Returns the current read sequence number.
   *
   * @return the read sequence
   */
  public long readSequence() {
    return readSequence.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E peek() {
    return isEmpty() ? null : buffer.get(wrap(readSequence.get())).getValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SneakyThrows
  @Locked("reentrantLock")
  public void put(final E element) {
    while (isFull()) {
      waitingProducer.await();
    }

    final long prevWriteSeq = writeSequence.get();
    final long nextWriteSeq = avoidSequenceOverflow(prevWriteSeq) + 1;

    buffer.get(wrap(nextWriteSeq)).setValue(element);

    writeSequence.compareAndSet(prevWriteSeq, nextWriteSeq);

    size.incrementAndGet();

    waitingConsumer.signal();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SneakyThrows
  @Locked("reentrantLock")
  public E take() {
    while (isEmpty()) {
      waitingConsumer.await();
    }

    final long prevReadSeq = readSequence.get();
    final long nextReadSeq = avoidSequenceOverflow(prevReadSeq) + 1;

    final E nextValue = buffer.get(wrap(prevReadSeq)).getValue();

    buffer.get(wrap(prevReadSeq)).setValue(null);

    readSequence.compareAndSet(prevReadSeq, nextReadSeq);

    size.decrementAndGet();

    waitingProducer.signal();

    return nextValue;
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public boolean offer(final E element) {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public boolean offer(final E element, final long timeout, final TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public E poll() {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public Iterator<E> iterator() {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public boolean add(final E element) {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public int drainTo(final Collection<? super E> collection) {
    throw new UnsupportedOperationException();
  }

  /**
   * @throws UnsupportedOperationException always
   */
  @Override
  public int drainTo(final Collection<? super E> collection, final int maxElements) {
    throw new UnsupportedOperationException();
  }

  @Getter
  @Setter
  static class Entry<E> {

    private E value;

  }

}
