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

package com.amazon.sqs.messaging.lib.exception;

import com.amazon.sqs.messaging.lib.model.RequestEntry;

import lombok.Getter;

/**
 * Exception thrown when a message exceeds the maximum allowed size (1MB).
 * Contains the original request entry for error handling.
 */
@Getter
@SuppressWarnings({ "rawtypes", "unchecked", "java:S1948" })
public class MaximumAllowedMessageException extends RuntimeException {

  private static final long serialVersionUID = -529663449633021689L;

  private final RequestEntry request;

  /**
   * Constructs a new exception with the given message and the offending request.
   *
   * @param string  the detail message
   * @param request the request entry that exceeded the size limit
   */
  public MaximumAllowedMessageException(final String string, final RequestEntry request) {
    super(string);
    this.request = request;
  }

  /**
   * Returns the request entry that caused this exception.
   *
   * @param <T> the payload type
   * @return the request entry
   */
  public <T> RequestEntry<T> getRequest() {
    return request;
  }

}
