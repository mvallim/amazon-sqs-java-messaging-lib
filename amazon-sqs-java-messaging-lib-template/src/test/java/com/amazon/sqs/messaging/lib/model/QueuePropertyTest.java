package com.amazon.sqs.messaging.lib.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.amazon.sqs.messaging.lib.model.QueueProperty.QueuePropertyBuilder;

// @formatter:off
class QueuePropertyTest {

  private static final String VALID_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";
  private static final String VALID_FIFO_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo";

  private QueuePropertyBuilder validBuilder() {
    return QueueProperty.builder()
      .fifo(false)
      .maximumPoolSize(5)
      .queueUrl(VALID_QUEUE_URL)
      .linger(10L)
      .maxBatchSize(10);
  }

  private QueuePropertyBuilder validFifoBuilder() {
    return QueueProperty.builder()
      .fifo(true)
      .maximumPoolSize(1)
      .queueUrl(VALID_FIFO_QUEUE_URL)
      .linger(10L)
      .maxBatchSize(10);
  }

  private QueuePropertyBuilder validBuilderWithoutLinger() {
    return QueueProperty.builder()
      .fifo(false)
      .maximumPoolSize(5)
      .queueUrl(VALID_QUEUE_URL)
      .maxBatchSize(10);
  }

  @Test
  void testBuildsSuccessfullyWithValidProperties() {
    final QueueProperty queueProperty = validBuilder().build();

    assertThat(queueProperty.isFifo(), is(false));
    assertThat(queueProperty.getMaximumPoolSize(), is(equalTo(5)));
    assertThat(queueProperty.getQueueUrl(), is(equalTo(VALID_QUEUE_URL)));
    assertThat(queueProperty.getLinger(), is(equalTo(10L)));
    assertThat(queueProperty.getMaxBatchSize(), is(equalTo(10)));
  }

  @Test
  void testToBuilderReturnsEquivalentInstance() {
    final QueueProperty original = validBuilder().build();

    final QueueProperty copy = original.toBuilder().build();

    assertThat(copy.isFifo(), is(equalTo(original.isFifo())));
    assertThat(copy.getMaximumPoolSize(), is(equalTo(original.getMaximumPoolSize())));
    assertThat(copy.getQueueUrl(), is(equalTo(original.getQueueUrl())));
    assertThat(copy.getLinger(), is(equalTo(original.getLinger())));
    assertThat(copy.getMaxBatchSize(), is(equalTo(original.getMaxBatchSize())));
  }

  @Test
  void testThrowsWhenMaximumPoolSizeIsNull() {
    final QueuePropertyBuilder builder = validBuilder().maximumPoolSize(null);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' is required"));
  }

  @Test
  void testThrowsWhenMaximumPoolSizeIsZero() {
    final QueuePropertyBuilder builder = validBuilder().maximumPoolSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be greater than zero"));
  }

  @Test
  void testThrowsWhenMaximumPoolSizeIsNegative() {
    final QueuePropertyBuilder builder = validBuilder().maximumPoolSize(-1);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be greater than zero"));
  }

  @Test
  void testThrowsWhenQueueUrlIsNull() {
    final QueuePropertyBuilder builder = validBuilder().queueUrl(null);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlIsEmpty() {
    final QueuePropertyBuilder builder = validBuilder().queueUrl("");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlIsBlank() {
    final QueuePropertyBuilder builder = validBuilder().queueUrl("   ");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlHasInvalidFormat() {
    final QueuePropertyBuilder builder = validBuilder().queueUrl("not-a-valid-url");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' must have the correct url format 'http(s)://{host}(:{port})/{account-id}/{queue-name}'"));
  }

  @Test
  void testThrowsWhenQueueUrlAccountIdIsNotTwelveDigits() {
    final QueuePropertyBuilder builder = validBuilder().queueUrl("https://sqs.us-east-1.amazonaws.com/123/my-queue");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' must have the correct url format 'http(s)://{host}(:{port})/{account-id}/{queue-name}'"));
  }

  @Test
  void testThrowsWhenLingerIsLessThanTen() {
    final QueuePropertyBuilder builder = validBuilder().linger(9L);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'linger' must be greater than or equal to 10 (ten)"));
  }

  @Test
  void testThrowsWhenLingerIsNegative() {
    final QueuePropertyBuilder builder = validBuilder().linger(-1L);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'linger' must be greater than or equal to 10 (ten)"));
  }

  @Test
  void testBuildsSuccessfullyWhenLingerIsExactlyTen() {
    final QueueProperty queueProperty = validBuilder().linger(10L).build();

    assertThat(queueProperty.getLinger(), is(equalTo(10L)));
  }

  @Test
  void testDefaultsLingerToTenWhenNotExplicitlySet() {
    final QueueProperty queueProperty = validBuilderWithoutLinger().build();

    assertThat(queueProperty.getLinger(), is(equalTo(10L)));
  }

  @Test
  void testExplicitLingerOverridesDefaultValue() {
    final QueueProperty queueProperty = validBuilderWithoutLinger().linger(42L).build();

    assertThat(queueProperty.getLinger(), is(equalTo(42L)));
  }

  @Test
  void testThrowsWhenLingerIsExplicitlySetBelowDefaultAndNotDefaulted() {
    final QueuePropertyBuilder builder = validBuilderWithoutLinger().linger(5L);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'linger' must be greater than or equal to 10 (ten)"));
  }

  @Test
  void testToBuilderPreservesDefaultedLingerValue() {
    final QueueProperty original = validBuilderWithoutLinger().build();

    final QueueProperty copy = original.toBuilder().build();

    assertThat(copy.getLinger(), is(equalTo(10L)));
  }

  @Test
  void testThrowsWhenMaxBatchSizeIsLessThanOne() {
    final QueuePropertyBuilder builder = validBuilder().maxBatchSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maxBatchSize' must be in the range of 1 (one) to 10 (ten)"));
  }

  @Test
  void testThrowsWhenMaxBatchSizeIsGreaterThanTen() {
    final QueuePropertyBuilder builder = validBuilder().maxBatchSize(11);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maxBatchSize' must be in the range of 1 (one) to 10 (ten)"));
  }

  @Test
  void testBuildsSuccessfullyWhenMaxBatchSizeIsAtLowerBoundary() {
    final QueueProperty queueProperty = validBuilder().maxBatchSize(1).build();

    assertThat(queueProperty.getMaxBatchSize(), is(equalTo(1)));
  }

  @Test
  void testBuildsSuccessfullyWhenMaxBatchSizeIsAtUpperBoundary() {
    final QueueProperty queueProperty = validBuilder().maxBatchSize(10).build();

    assertThat(queueProperty.getMaxBatchSize(), is(equalTo(10)));
  }

  @Test
  void testBuildsSuccessfullyWhenFifoTrueWithMaximumPoolSizeOneAndFifoSuffixedUrl() {
    final QueueProperty queueProperty = validFifoBuilder().build();

    assertThat(queueProperty.isFifo(), is(true));
    assertThat(queueProperty.getMaximumPoolSize(), is(equalTo(1)));
    assertThat(queueProperty.getQueueUrl(), is(equalTo(VALID_FIFO_QUEUE_URL)));
  }

  @Test
  void testThrowsWhenFifoTrueAndMaximumPoolSizeIsNotOne() {
    final QueuePropertyBuilder builder = validFifoBuilder().maximumPoolSize(2);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be equal to 1 (one) when 'fifo' is true"));
  }

  @Test
  void testThrowsWhenFifoTrueAndQueueUrlDoesNotEndWithFifoSuffix() {
    final QueuePropertyBuilder builder = validFifoBuilder().queueUrl(VALID_QUEUE_URL);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' must be ends with in '.fifo' when 'fifo' is true"));
  }

  @Test
  void testExceptionIsThrownWithNonEmptyValidationMessageWhenMultipleFieldsAreInvalid() {
    final QueuePropertyBuilder builder = QueueProperty.builder().fifo(false).maximumPoolSize(null).queueUrl(null).linger(0L).maxBatchSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), is(notNullValue()));
  }

  @Test
  void testToStringDoesNotThrow() {
    final QueueProperty queueProperty = validBuilder().build();

    assertThat(queueProperty.toString(), is(notNullValue()));
  }

}
// @formatter:on