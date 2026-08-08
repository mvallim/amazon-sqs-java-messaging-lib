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

import static br.com.fluentvalidator.predicate.ComparablePredicate.betweenInclusive;
import static br.com.fluentvalidator.predicate.ComparablePredicate.equalTo;
import static br.com.fluentvalidator.predicate.ComparablePredicate.greaterThan;
import static br.com.fluentvalidator.predicate.ComparablePredicate.greaterThanOrEqual;
import static br.com.fluentvalidator.predicate.LogicalPredicate.not;
import static br.com.fluentvalidator.predicate.ObjectPredicate.nullValue;
import static br.com.fluentvalidator.predicate.StringPredicate.stringMatches;
import static java.util.function.Function.identity;

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import br.com.fluentvalidator.AbstractValidator;
import br.com.fluentvalidator.context.ValidationResult;
import br.com.fluentvalidator.predicate.PredicateBuilder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

// @formatter:off
/**
 * Configuration properties for an Amazon SQS queue.
 */
@Getter
@ToString
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class QueueProperty {

  private static final long DEFAULT_LINGER = 10L;

  /**
   * Whether the queue is a FIFO queue.
   */
  private final boolean fifo;

  /**
   * The maximum number of threads in the pool for concurrent publishing.
   */
  private final Integer maximumPoolSize;

  /**
   * The URL of the SQS queue.
   */
  private final String queueUrl;

  /**
   * The batching linger time in milliseconds.
   */
  private final long linger;

  /**
   * The maximum number of messages per batch.
   */
  private final int maxBatchSize;

  /**
   * Validates {@link QueueProperty} instances against the SQS constraints for
   * pool size, queue URL format, linger, batch size, and FIFO consistency.
   */
  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  static final class QueuePropertyValidator extends AbstractValidator<QueueProperty> {

    public static final QueuePropertyValidator INSTANCE = new QueuePropertyValidator();

    @Override
    public void rules() {

      failFastRule();

      ruleFor("maximumPoolSize", QueueProperty::getMaximumPoolSize)
        .must(not(nullValue()))
          .withMessage("'maximumPoolSize' is required")
        .must(greaterThan(0))
          .when(not(nullValue()))
          .withMessage("'maximumPoolSize' must be greater than zero");

      ruleFor("queueUrl", QueueProperty::getQueueUrl)
        .must(StringUtils::isNotBlank)
          .withMessage("'queueUrl' is required")
        .must(stringMatches("^https?://[^/]+(?::\\d+)?/\\d{12}/([\\w-]{1,80}|[\\w-]{1,75}\\.fifo)$"))
          .when(StringUtils::isNotBlank)
          .withMessage("'queueUrl' must have the correct url format 'http(s)://{host}(:{port})/{account-id}/{queue-name}'");

      ruleFor("linger", QueueProperty::getLinger)
        .must(greaterThanOrEqual(DEFAULT_LINGER))
          .withMessage("'linger' must be greater than or equal to 10 (ten)");

      ruleFor("maxBatchSize", QueueProperty::getMaxBatchSize)
        .must(betweenInclusive(1, 10))
          .withMessage("'maxBatchSize' must be in the range of 1 (one) to 10 (ten)");

      ruleFor(identity())
        .must(equalTo(QueueProperty::getMaximumPoolSize, 1))
          .when(QueueProperty::isFifo)
          .withFieldName("maximumPoolSize")
          .withMessage("'maximumPoolSize' must be equal to 1 (one) when 'fifo' is true")
          .withAttempedValue(QueueProperty::getMaximumPoolSize)
        .must(stringEndsWith(QueueProperty::getQueueUrl, ".fifo"))
          .when(QueueProperty::isFifo)
          .withFieldName("queueUrl")
          .withMessage("'queueUrl' must be ends with in '.fifo' when 'fifo' is true")
          .withAttempedValue(QueueProperty::getQueueUrl);
    }

    private static <T> Predicate<T> stringEndsWith(final Function<T, String> source, final String ends) {
      return PredicateBuilder.<T>from(not(nullValue())).and(obj -> source.apply(obj).endsWith(ends));
    }

  }

  /**
   * Builder for {@link QueueProperty} that applies the default linger value when
   * none is explicitly configured and validates the assembled properties.
   */
  @SuppressWarnings("java:S116")
  public static class QueuePropertyBuilder {

    /**
     * Tracks whether {@code linger(long)} was explicitly invoked.
     *
     * <p>This flag allows applying {@link QueueProperty#DEFAULT_LINGER} only when
     * no explicit value was provided through the builder.
     */
    private boolean linger$set;

    /**
     * Sets the batching linger time in milliseconds.
     *
     * @param linger the linger time in milliseconds
     * @return this builder
     */
    public QueuePropertyBuilder linger(final long linger) {
      this.linger = linger;
      linger$set = true;
      return this;
    }

    /**
     * Builds the {@link QueueProperty}, applying the default linger value if none
     * was explicitly set and validating the result.
     *
     * @return the validated queue property
     * @throws IllegalArgumentException if the assembled properties fail validation
     */
    public QueueProperty build() {
      final long linger = linger$set ? this.linger : DEFAULT_LINGER;

      final QueueProperty queueProperty = new QueueProperty(fifo, maximumPoolSize, queueUrl, linger, maxBatchSize);

      final ValidationResult validationResult = QueuePropertyValidator.INSTANCE.validate(queueProperty);

      if (CollectionUtils.isNotEmpty(validationResult.getErrors())) {
        throw new IllegalArgumentException(validationResult.toString());
      }

      return queueProperty;
    }

  }

}
// @formatter:on