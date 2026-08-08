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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.sqs.messaging.lib.concurrent.ThreadFactoryProvider;
import com.amazon.sqs.messaging.lib.model.RequestEntry;
import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

//@formatter:off
/**
 * Abstract base class for producing messages to an Amazon SQS queue. Enqueues
 * request entries and tracks their completion via {@link ListenableFuture}.
 *
 * @param <E> the request entry payload type
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class AbstractAmazonSqsProducer<E> implements AmazonSqsProducer<E> {

  /** Class logger. */
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAmazonSqsProducer.class);

  /** The producer lifecycle state, initially {@link State#RUNNING}. */
  private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

  /** Map of pending requests keyed by request ID for asynchronous completion. */
  private final ConcurrentMap<String, ListenableFuture<ResponseSuccessEntry, ResponseFailEntry>> pendingRequests;

  /** The blocking queue for buffering requests before batch processing. */
  private final BlockingQueue<RequestEntry<E>> queueRequests;

  private final ExecutorService callbackExecutor = Executors.newCachedThreadPool(ThreadFactoryProvider.getThreadFactory());

  /**
   * Sends a request entry by enqueuing it for batch processing.
   *
   * @param requestEntry the request entry to send
   * @return a {@link ListenableFuture} for tracking the send result
   */
  @Override
  public ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> send(final RequestEntry<E> requestEntry) {
    if (State.RUNNING.equals(state.get())) {
      return enqueueRequest(requestEntry);
    } else {
      final ListenableFutureImpl listenableFutureImpl = new ListenableFutureImpl(Runnable::run);

      listenableFutureImpl.fail(ResponseFailEntry.builder()
        .withCode("000")
        .withId(requestEntry.getId())
        .withMessage(String.format("Producer is currently in %s mode; no further messages will be accepted.", state.get().name()))
        .withSenderFault(true)
        .build());

      return listenableFutureImpl;
    }
  }

  /**
   * Transitions the producer to the shutdown state. No further messages will be
   * accepted once shutdown.
   *
   * @param runnable a callback invoked after the producer transitions to the shutdown state
   */
  @Override
  public void shutdown(final Runnable runnable) {
    state.compareAndSet(State.RUNNING, State.SHUTDOWN);

    runnable.run();

    try {
      LOGGER.warn("Shutdown producer {}", getClass().getSimpleName());

      callbackExecutor.shutdown();
      if (!callbackExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        LOGGER.warn("Producer executor service did not terminate in the specified time.");
        final List<Runnable> droppedTasks = callbackExecutor.shutdownNow();
        LOGGER.warn("Producer executor service was abruptly shut down. {} tasks will not be executed.", droppedTasks.size());
      }
    } catch (final InterruptedException ex) {
      LOGGER.error(ex.getMessage(), ex);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Enqueues a request entry and registers a pending future for tracking.
   *
   * @param requestEntry the request entry to enqueue
   * @return a {@link ListenableFuture} for tracking the result
   */
  @SneakyThrows
  private ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> enqueueRequest(final RequestEntry<E> requestEntry) {
    try {
      final ListenableFuture<ResponseSuccessEntry, ResponseFailEntry> trackPendingRequest = new ListenableFutureImpl(callbackExecutor);
      pendingRequests.put(requestEntry.getId(), trackPendingRequest);
      queueRequests.put(requestEntry);
      return trackPendingRequest;
    } catch (final InterruptedException ex) {
      pendingRequests.remove(requestEntry.getId());
      Thread.currentThread().interrupt();
      throw ex;
    }
  }

  /**
   * Lifecycle states of the producer.
   */
  enum State {
    RUNNING, SHUTDOWN
  }

}
// @formatter:on
