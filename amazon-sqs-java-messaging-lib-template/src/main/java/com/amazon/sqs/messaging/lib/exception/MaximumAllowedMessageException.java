package com.amazon.sqs.messaging.lib.exception;

import com.amazon.sqs.messaging.lib.model.RequestEntry;

import lombok.Getter;

/**
 * Exception thrown when a message exceeds the maximum allowed size (1MB).
 * Contains the original request entry for error handling.
 */
@Getter
@SuppressWarnings({ "rawtypes", "unchecked" })
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
