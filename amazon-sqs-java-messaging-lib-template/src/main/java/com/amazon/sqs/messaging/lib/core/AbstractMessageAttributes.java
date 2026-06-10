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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

// @formatter:off
/**
 * Abstract base class for converting message headers into typed message attributes
 * suitable for Amazon SQS. Supports Enum, String, Number, ByteBuffer, and List values.
 *
 * @param <V> the message attribute value type (e.g., SDK-specific type)
 */
@SuppressWarnings("java:S6204")
abstract class AbstractMessageAttributes<V> {

  protected static final String BINARY = "Binary";

  protected static final String STRING = "String";

  protected static final String NUMBER = "Number";

  protected static final String STRING_ARRAY = "String.Array";

  /**
   * Converts a map of message headers into typed message attributes.
   *
   * @param messageHeaders the headers to convert
   * @return a map of attribute names to typed attribute values
   */
  public Map<String, V> messageAttributes(final Map<String, Object> messageHeaders) {
    final Map<String, V> messageAttributes = new HashMap<>();

    for (final Entry<String, Object> messageHeader : messageHeaders.entrySet()) {
      final String key = messageHeader.getKey();
      final Object value = messageHeader.getValue();

      if (value instanceof Enum) {
        messageAttributes.put(key, getEnumMessageAttribute(Enum.class.cast(value)));
      } else if (value instanceof String) {
        messageAttributes.put(key, getStringMessageAttribute(String.class.cast(value)));
      } else if (value instanceof Number) {
        messageAttributes.put(key, getNumberMessageAttribute(Number.class.cast(value)));
      } else if (value instanceof ByteBuffer) {
        messageAttributes.put(key, getBinaryMessageAttribute(ByteBuffer.class.cast(value)));
      } else if (value instanceof List) {
        messageAttributes.put(key, getStringArrayMessageAttribute(List.class.cast(value)));
      }
    }

    return messageAttributes;
  }

  /**
   * Formats a list of string values into a JSON-style array string.
   *
   * @param values the list of values to format
   * @return a string representation of the string array
   */
  protected static String stringArray(final List<?> values) {
    final List<String> collect = values.stream()
      .filter(String.class::isInstance)
      .map(String.class::cast)
      .map(value -> "\"" + value + "\"")
      .collect(Collectors.toList());
    return "[ " + String.join(", ", collect) + " ]";
  }

  /**
   * Converts an enum value to a message attribute.
   *
   * @param value the enum value
   * @return the typed message attribute
   */
  protected abstract V getEnumMessageAttribute(final Enum<?> value);

  /**
   * Converts a string value to a message attribute.
   *
   * @param value the string value
   * @return the typed message attribute
   */
  protected abstract V getStringMessageAttribute(final String value);

  /**
   * Converts a number value to a message attribute.
   *
   * @param value the number value
   * @return the typed message attribute
   */
  protected abstract V getNumberMessageAttribute(final Number value);

  /**
   * Converts a binary value to a message attribute.
   *
   * @param value the byte buffer value
   * @return the typed message attribute
   */
  protected abstract V getBinaryMessageAttribute(final ByteBuffer value);

  /**
   * Converts a list of values to a string array message attribute.
   *
   * @param value the list of values
   * @return the typed message attribute
   */
  protected abstract V getStringArrayMessageAttribute(final List<?> value);

}
// @formatter:on
