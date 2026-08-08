# amazon-sqs-java-messaging-lib-v2

AWS SDK v2 implementation of the Amazon SQS Java Messaging Library. Provides batched message sending to SQS using `software.amazon.awssdk:sqs:2.20.162`.

## Package Structure

```text
com.amazon.sqs.messaging.lib
  ├── core/
  │   ├── AmazonSqsTemplate.java         -- Public API entry point
  │   ├── AmazonSqsProducerImpl.java     -- Producer (enqueues requests)
  │   ├── AmazonSqsConsumerImpl.java     -- Consumer (calls SqsClient.sendMessageBatch)
  │   └── MessageAttributes.java         -- Header-to-MessageAttributeValue converter
  └── metrics/
      └── AmazonSqsConsumerMetricsDecorator.java  -- Micrometer metrics decorator
```

## Key Classes

| Class                               | Description                                                                                                                                                                                                                |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AmazonSqsTemplate<E>`              | Extends `AbstractAmazonSqsTemplate`. Primary API: `send()`, `shutdown()`, `await()`. Use the builder: `AmazonSqsTemplate.builder(sqsClient, queueProperty)`. Deprecated constructors available for backward compatibility. |
| `AmazonSqsProducerImpl<E>`          | Extends `AbstractAmazonSqsProducer`. Thin wrapper that enqueues `RequestEntry` into a shared blocking queue.                                                                                                               |
| `AmazonSqsConsumerImpl<E>`          | Extends `AbstractAmazonSqsConsumer`. Calls `SqsClient.sendMessageBatch()` with v2 `SendMessageBatchRequest`/`SendMessageBatchResponse`. Handles per-entry success/failure from batch response.                             |
| `MessageAttributes`                 | Extends `AbstractMessageAttributes<MessageAttributeValue>`. Converts header entries to v2 `MessageAttributeValue` objects (String, Number, Binary via `SdkBytes`, String.Array, Enum).                                    |
| `AmazonSqsConsumerMetricsDecorator` | Extends `AbstractAmazonSqsConsumerMetricsDecorator<SendMessageBatchRequest, SendMessageBatchResponse>`. Records publish attempts, latency, batch size, inflight count. Tags failures by `AwsServiceException` error code. |

## Dependencies

```text
com.github.mvallim:amazon-sqs-java-messaging-lib-template:1.3.2
software.amazon.awssdk:sqs:2.20.162
```

## Usage

### Standard SQS Queue

```java
SqsClient sqsClient = SqsClient.create();

QueueProperty queueProperty = QueueProperty.builder()
    .fifo(false)
    .linger(100)
    .maxBatchSize(10)
    .maximumPoolSize(20)
    .queueUrl("http://localhost:4566/000000000000/queue")
    .build();

AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(sqsClient, queueProperty).build();

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

AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(sqsClient, queueProperty).build();

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
AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(sqsClient, queueProperty)
    .objectMapper(new ObjectMapper())
    .queueRequests(new LinkedBlockingQueue<>(100))
    .publishDecorator(req -> req)
    .build();
```

### Using Micrometer Metrics

```java
MeterRegistry meterRegistry = new CompositeMeterRegistry();
meterRegistry.add(new JmxMeterRegistry());

AmazonSqsTemplate<MyMessage> sqsTemplate = AmazonSqsTemplate.builder(sqsClient, queueProperty)
    .meterRegistry(meterRegistry)
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
