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

package com.amazon.sqs.messaging.lib.model;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Represents a request entry to be sent to an SQS queue, containing the
 * payload, message headers, and optional FIFO attributes.
 *
 * @param <T> the payload type
 */
@Getter
@ToString
@Builder(toBuilder = true, setterPrefix = "with")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestEntry<T> {

  /**
   * The timestamp (in nanoseconds) when the request entry was created.
   */
  @NonNull
  @Builder.Default
  private final Long createTime = System.nanoTime();

  /**
   * The unique identifier of the request.
   */
  @NonNull
  @Builder.Default
  private final String id = UUID.randomUUID().toString();

  /**
   * The message payload.
   */
  private final T value;

  /**
   * The message headers to be sent as SQS message attributes.
   */
  @Builder.Default
  private final Map<String, Object> messageHeaders = Collections.emptyMap();

  /**
   * The message group ID for FIFO queues.
   */
  private final String groupId;

  /**
   * The message deduplication ID for FIFO queues.
   */
  private final String deduplicationId;

}
