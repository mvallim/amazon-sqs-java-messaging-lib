package com.amazon.sqs.messaging.lib.exception;

/**
 * Base exception indicating that a request entry could not be published because
 * it is invalid, either exceeding the maximum allowed message size or failing to
 * serialize. Concrete subtypes are created via the {@code from*} factory methods.
 */
public abstract class PoisonRequestEntryException extends Exception {

  private static final long serialVersionUID = -1884047816775456709L;

  /**
   * Creates a poison request entry exception with the given message.
   *
   * @param string the detail message
   */
  protected PoisonRequestEntryException(final String string) {
    super(string);
  }

  /**
   * Creates a poison request entry exception with the given message and cause.
   *
   * @param message the detail message
   * @param cause   the underlying cause of the failure
   */
  protected PoisonRequestEntryException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a poison request entry exception for a message that exceeds the
   * maximum allowed size.
   *
   * @param message the detail message
   * @return a {@link MaximumAllowedMessageException}
   */
  public static PoisonRequestEntryException fromMaximumAllowedMessage(final String message) {
    return new MaximumAllowedMessageException(message);
  }

  /**
   * Creates a poison request entry exception for a payload that failed to
   * serialize.
   *
   * @param message   the detail message
   * @param throwable the underlying serialization exception
   * @return a {@link JsonProcessingException}
   */
  public static PoisonRequestEntryException fromJsonProcessing(final String message, final Throwable throwable) {
    return new JsonProcessingException(message, throwable);
  }

}
