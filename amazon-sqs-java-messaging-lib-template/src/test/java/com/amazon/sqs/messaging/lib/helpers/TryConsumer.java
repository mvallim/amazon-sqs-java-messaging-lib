package com.amazon.sqs.messaging.lib.helpers;

@FunctionalInterface
public interface TryConsumer<T> {

  void accept(T t) throws Exception;
}