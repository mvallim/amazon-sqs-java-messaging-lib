# amazon-sqs-java-messaging-lib-v1

AWS SDK v1 implementation of the Amazon SQS Java Messaging Library. Provides batched message sending to SQS using `com.amazonaws:aws-java-sdk-sqs:1.12.661`.

## Package Structure

```text
com.amazon.sqs.messaging.lib
  ├── core/
  │   ├── AmazonSqsTemplate.java         -- Public API entry point
  │   ├── AmazonSqsProducerImpl.java     -- Producer (enqueues requests)
  │   ├── AmazonSqsConsumerImpl.java     -- Consumer (calls AmazonSQS.sendMessageBatch)
  │   └── MessageAttributes.java         -- Header-to-MessageAttributeValue converter
  └── metrics/
      └── AmazonSqsConsumerMetricsDecorator.java  -- Micrometer metrics decorator
```

## Key Classes

| Class                               | Description                                                                                                                                                                                                                |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AmazonSqsTemplate<E>`              | Extends `AbstractAmazonSqsTemplate`. Primary API: `send()`, `shutdown()`, `await()`. Use the builder: `AmazonSqsTemplate.builder(amazonSQS, queueProperty)`. Deprecated constructors available for backward compatibility. |
| `AmazonSqsProducerImpl<E>`          | Extends `AbstractAmazonSqsProducer`. Thin wrapper that enqueues `RequestEntry` into a shared blocking queue.                                                                                                               |
| `AmazonSqsConsumerImpl<E>`          | Extends `AbstractAmazonSqsConsumer`. Calls `AmazonSQS.sendMessageBatch()` with v1 `SendMessageBatchRequest`/`SendMessageBatchResult`. Handles per-entry success/failure from batch response.                               |
| `MessageAttributes`                 | Extends `AbstractMessageAttributes<MessageAttributeValue>`. Converts header entries to v1 `MessageAttributeValue` objects (String, Number, Binary, String.Array, Enum).                                                    |
| `AmazonSqsConsumerMetricsDecorator` | Extends `AbstractAmazonSqsConsumerMetricsDecorator<SendMessageBatchRequest, SendMessageBatchResult>`. Records publish attempts, latency, batch size, inflight count. Tags failures by `AmazonServiceException` error code.  |

## Dependencies

```text
com.github.mvallim:amazon-sqs-java-messaging-lib-template:1.3.2
com.amazonaws:aws-java-sdk-sqs:1.12.661
```

## Usage

### Standard SQS Queue

```java
AmazonSQS amazonSQS = AmazonSQSClientBuilder.defaultClient();

QueueProperty queueProperty = QueueProperty.builder()
    .fifo(false)
    .linger(100)
    .maxBatchSize(10)
    .maximumPoolSize(20)
    .queueUrl("http://localhost:4566/000000000000/queue")
    .build();

AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(amazonSQS, queueProperty).build();

RequestEntry<MyMessage> entry = RequestEntry.<MyMessage>builder()
    .withValue(new MyMessage())
    .withMessageHeaders(Map.of("header1", "value1"))
    .build();

sqsTemplate.send(entry);
sqsTemplate.shutdown();
```

### FIFO SQS Queue

```java
QueueProperty queueProperty = QueueProperty.builder()
    .fifo(true)
    .linger(100)
    .maxBatchSize(10)
    .maximumPoolSize(1)
    .queueUrl("http://localhost:4566/000000000000/queue.fifo")
    .build();

AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(amazonSQS, queueProperty).build();

RequestEntry<MyMessage> entry = RequestEntry.<MyMessage>builder()
    .withValue(new MyMessage())
    .withGroupId(UUID.randomUUID().toString())
    .withDeduplicationId(UUID.randomUUID().toString())
    .build();

sqsTemplate.send(entry).addCallback(
    success -> LOGGER.info("Sent: {}", success),
    failure -> LOGGER.error("Failed: {}", failure)
);
sqsTemplate.await().join();
```

### With Custom ObjectMapper and Queue

```java
AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(amazonSQS, queueProperty)
    .objectMapper(new ObjectMapper())
    .queueRequests(new LinkedBlockingQueue<>(100))
    .publishDecorator(req -> req)
    .build();
```

## Metrics

When a `MeterRegistry` is provided via `.meterRegistry()`, the following metrics are recorded:

| Metric                         | Type                | Tags                                | Description                    |
|--------------------------------|---------------------|-------------------------------------|--------------------------------|
| `sqs.publish.attempts`         | Counter             | `queue`                             | Total SendMessageBatch attempts |
| `sqs.publish.success`          | Counter             | `queue`                             | Successful messages            |
| `sqs.publish.failure`          | Counter             | `queue`, `error_code`, `error_type` | Failed messages                |
| `sqs.publish.duration`         | Timer (p50/p95/p99) | `queue`                             | Publish latency                |
| `sqs.publish.batch.size`       | DistributionSummary | `queue`                             | Messages per batch             |
| `sqs.publish.inflight`         | Gauge               | `queue`                             | In-flight publish batches      |
| `blocking.queue.puts.total`    | Counter             | `name`                              | Successful put operations      |
| `blocking.queue.puts.failed`   | Counter             | `name`                              | Failed put operations          |
| `blocking.queue.put.duration`  | Timer               | `name`                              | Put latency                    |
| `blocking.queue.takes.total`   | Counter             | `name`                              | Successful take operations     |
| `blocking.queue.takes.failed`  | Counter             | `name`                              | Failed take operations         |
| `blocking.queue.take.duration` | Timer               | `name`                              | Take latency                   |
| `blocking.queue.size`          | Gauge               | `name`                              | Queue depth                    |
| `executor.active`              | Gauge               | `name`                              | Active tasks                   |
| `executor.tasks.succeeded`     | Counter             | `name`                              | Successful tasks               |
| `executor.tasks.failed`        | Counter             | `name`                              | Failed tasks                   |
| `executor.task.duration`       | Timer               | `name`                              | Task duration                  |

See the [Technical Guide](../GUIDE.md#metrics-micrometer) for details.
