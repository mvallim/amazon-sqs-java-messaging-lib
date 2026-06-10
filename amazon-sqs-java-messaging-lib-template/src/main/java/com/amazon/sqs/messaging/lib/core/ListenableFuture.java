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

import java.util.function.Consumer;

import com.amazon.sqs.messaging.lib.model.ResponseFailEntry;
import com.amazon.sqs.messaging.lib.model.ResponseSuccessEntry;

/**
 * A listenable future that supports registering success and failure callbacks
 * for asynchronous result handling.
 *
 * @param <S> the success result type
 * @param <F> the failure result type
 */
public interface ListenableFuture<S, F> {

  /**
   * Registers callbacks for success and failure outcomes.
   *
   * @param successCallback the callback invoked on success
   * @param failureCallback the callback invoked on failure
   */
  void addCallback(final Consumer<? super S> successCallback, final Consumer<? super F> failureCallback);

  /**
   * Registers a success callback with a no-op failure callback.
   *
   * @param successCallback the callback invoked on success
   */
  default void addCallback(final Consumer<? super S> successCallback) {
    addCallback(successCallback, result -> {
    });
  }

  /**
   * Marks the future as successfully completed.
   *
   * @param entry the success entry
   */
  void success(final ResponseSuccessEntry entry);

  /**
   * Marks the future as failed.
   *
   * @param entry the failure entry
   */
  void fail(final ResponseFailEntry entry);

}