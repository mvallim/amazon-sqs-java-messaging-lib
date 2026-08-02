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
    return QueueProperty.builder().fifo(false).maximumPoolSize(5).queueUrl(VALID_QUEUE_URL).linger(10L).maxBatchSize(10);
  }

  @Test
  void testBuildsSuccessfullyWithValidProperties() {
    final QueueProperty QueueProperty = validBuilder().build();

    assertThat(QueueProperty.isFifo(), is(false));
    assertThat(QueueProperty.getMaximumPoolSize(), is(equalTo(5)));
    assertThat(QueueProperty.getQueueUrl(), is(equalTo(VALID_QUEUE_URL)));
    assertThat(QueueProperty.getLinger(), is(equalTo(10L)));
    assertThat(QueueProperty.getMaxBatchSize(), is(equalTo(10)));
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
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().maximumPoolSize(null);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' is required"));
  }

  @Test
  void testThrowsWhenMaximumPoolSizeIsZero() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().maximumPoolSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be greater than zero"));
  }

  @Test
  void testThrowsWhenMaximumPoolSizeIsNegative() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().maximumPoolSize(-1);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be greater than zero"));
  }

  @Test
  void testThrowsWhenFifoTrueAndMaximumPoolSizeIsNotOne() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().fifo(true).maximumPoolSize(2).queueUrl(VALID_FIFO_QUEUE_URL);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maximumPoolSize' must be equal to 1 (one) when 'fifo' is true"));
  }

  @Test
  void testBuildsSuccessfullyWhenFifoTrueAndMaximumPoolSizeIsOne() {
    final QueueProperty QueueProperty = validBuilder().fifo(true).maximumPoolSize(1).queueUrl(VALID_FIFO_QUEUE_URL).build();

    assertThat(QueueProperty.isFifo(), is(true));
    assertThat(QueueProperty.getMaximumPoolSize(), is(equalTo(1)));
    assertThat(QueueProperty.getQueueUrl(), is(equalTo(VALID_FIFO_QUEUE_URL)));
  }

  @Test
  void testThrowsWhenqueueUrlIsNull() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().queueUrl(null);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlIsEmpty() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().queueUrl("");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlIsBlank() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().queueUrl("   ");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' is required"));
  }

  @Test
  void testThrowsWhenQueueUrlHasInvalidFormat() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().queueUrl("not-a-valid-url");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' must have the correct url format 'http(s)://{host}(:{port})/{account-id}/{queue-name}'"));
  }

  @Test
  void testThrowsWhenQueueUrlAccountIdIsNotTwelveDigits() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().queueUrl("https://sqs.us-east-1.amazonaws.com/123/my-queue");

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'queueUrl' must have the correct url format 'http(s)://{host}(:{port})/{account-id}/{queue-name}'"));
  }

  @Test
  void testBuildsSuccessfullyWithFifoQueueUrlSuffix() {
    final QueueProperty QueueProperty = validBuilder().fifo(true).maximumPoolSize(1).queueUrl(VALID_FIFO_QUEUE_URL).build();

    assertThat(QueueProperty.getQueueUrl(), is(equalTo(VALID_FIFO_QUEUE_URL)));
  }

  @Test
  void testThrowsWhenLingerIsLessThanTen() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().linger(9L);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'linger' must be greater than or equal to 10 (ten)"));
  }

  @Test
  void testThrowsWhenLingerIsNegative() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().linger(-1L);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'linger' must be greater than or equal to 10 (ten)"));
  }

  @Test
  void testBuildsSuccessfullyWhenLingerIsExactlyTen() {
    final QueueProperty QueueProperty = validBuilder().linger(10L).build();

    assertThat(QueueProperty.getLinger(), is(equalTo(10L)));
  }

  @Test
  void testThrowsWhenMaxBatchSizeIsLessThanOne() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().maxBatchSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maxBatchSize' must be in the range of 1 (one) to 10 (ten)"));
  }

  @Test
  void testThrowsWhenMaxBatchSizeIsGreaterThanTen() {
    final QueueProperty.QueuePropertyBuilder builder = validBuilder().maxBatchSize(11);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), containsString("'maxBatchSize' must be in the range of 1 (one) to 10 (ten)"));
  }

  @Test
  void testBuildsSuccessfullyWhenMaxBatchSizeIsAtLowerBoundary() {
    final QueueProperty QueueProperty = validBuilder().maxBatchSize(1).build();

    assertThat(QueueProperty.getMaxBatchSize(), is(equalTo(1)));
  }

  @Test
  void testBuildsSuccessfullyWhenMaxBatchSizeIsAtUpperBoundary() {
    final QueueProperty QueueProperty = validBuilder().maxBatchSize(10).build();

    assertThat(QueueProperty.getMaxBatchSize(), is(equalTo(10)));
  }

  @Test
  void testExceptionIsThrownWithNonEmptyValidationMessageWhenMultipleFieldsAreInvalid() {
    final QueueProperty.QueuePropertyBuilder builder = QueueProperty.builder().fifo(false).maximumPoolSize(null).queueUrl(null).linger(0L).maxBatchSize(0);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, builder::build);

    assertThat(exception.getMessage(), is(notNullValue()));
  }

  @Test
  void testToStringDoesNotThrow() {
    final QueueProperty QueueProperty = validBuilder().build();

    assertThat(QueueProperty.toString(), is(notNullValue()));
  }

}
// @formatter:on