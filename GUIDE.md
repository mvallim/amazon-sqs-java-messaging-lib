# Amazon SQS Java Messaging Library — Technical Guide

## Architecture Overview

The library provides an asynchronous, batched messaging client for Amazon SQS, supporting both AWS SDK v1 and v2. It is organized as a multi-module Maven project:

| Module                                   | Artifact                           | Purpose                                                  |
|------------------------------------------|------------------------------------|----------------------------------------------------------|
| `amazon-sqs-java-messaging-lib-template` | *(internal)*                       | SDK-agnostic core: batching, queuing, threading, metrics |
| `amazon-sqs-java-messaging-lib-v1`       | `amazon-sqs-java-messaging-lib-v1` | AWS SDK v1 implementation (`AmazonSQS` client)           |
| `amazon-sqs-java-messaging-lib-v2`       | `amazon-sqs-java-messaging-lib-v2` | AWS SDK v2 implementation (`SqsClient`)                  |

### Core Components

```text
┌────────────────────────────────────────────────────────────┐
│                    AmazonSqsTemplate<E>                    │
├────────────────────────────────────────────────────────────┤
│  ┌───────────────────────┐    ┌─────────────────────────┐  │
│  │  AmazonSqsProducer<E> │    │  AmazonSqsConsumer<R,O> │  │
│  │  (AbstractProducer)   │    │  (AbstractConsumer)     │  │
│  │                       │    │                         │  │
│  │  - BlockingQueue      │    │  - ScheduledExecutor    │  │
│  │  - PendingRequests    │    │  - Batching Logic       │  │
│  └──────────┬────────────┘    └───────────┬─────────────┘  │
│             │                             │                │
│         send(E)                   sendMessageBatch(...)    │
└─────────────┼─────────────────────────────┼────────────────┘
              │                             │
              ▼                             ▼
      ┌───────────────────────────────────────────┐
      │            Amazon SQS (v1/v2)             │
      └───────────────────────────────────────────┘
```

- **`AmazonSqsTemplate`** — Main entry point. Created via a fluent builder (`AmazonSqsTemplate.builder(sqsClient, queueProperty)`).
- **`AmazonSqsProducer`** — Accepts messages into a `BlockingQueue`, tracks pending futures, and returns `ListenableFuture` results.
- **`AmazonSqsConsumer`** — Scheduled drainer that pulls messages from the queue at `linger` intervals, batches them (respecting count and 1024KB size limits), and sends them via the SDK's `sendMessageBatch` API.

---

## Batching Behavior

Messages are accumulated in a `BlockingQueue` and drained periodically by a scheduled executor.

- **Linger**: Time (ms) to wait before flushing the batch. Resets on each new message arrival.
- **Max batch size**: Maximum number of messages per `sendMessageBatch` call.
- **1024KB limit**: Each batch request must not exceed the SQS payload limit. Messages exceeding 1024KB individually throw `MaximumAllowedMessageException`.
- **Memory**: The buffer stores up to `maximumPoolSize × maxBatchSize` messages internally (backed by the `BlockingQueue`). The default `RingBufferBlockingQueue` rounds its capacity up to the next power of two, so the *actual* allocated capacity may be up to ~2x the computed value.

For **FIFO** queues, messages are sent **synchronously** on a single-threaded executor to preserve ordering. For **standard** queues, sending is **asynchronous** via a multi-threaded executor.

---

## Message Flow

1. User calls `template.send(RequestEntry<E>)`
2. Producer serializes the message payload to JSON (via Jackson `ObjectMapper`)
3. Producer enqueues the serialized entry into a `BlockingQueue` and registers a `ListenableFuture`
4. Consumer's scheduled task drains the queue at `linger` intervals, building a `SendMessageBatchRequest`
5. Consumer calls `sendMessageBatch()` on the SQS client
6. On success: individual `ResponseSuccessEntry` results are matched back to futures by message ID
7. On failure: `ResponseFailEntry` objects complete the corresponding futures with error details

---

## Dependencies

### Template Module (shared)

```xml
<dependencies>
    <dependency>org.slf4j:slf4j-api:2.0.6</dependency>
    <dependency>org.apache.commons:commons-collections4:4.5.0</dependency>
    <dependency>org.apache.commons:commons-lang3:3.20.0</dependency>
    <dependency>com.fasterxml.jackson.core:jackson-databind:2.16.1</dependency>
    <dependency>io.micrometer:micrometer-core:1.16.3</dependency>
    <dependency>org.projectlombok:lombok:1.18.42 (provided)</dependency>
</dependencies>
```

### AWS SDK v1 Module

```xml
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>aws-java-sdk-sqs</artifactId>
    <version>1.12.661</version>
</dependency>
```

### AWS SDK v2 Module

```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sqs</artifactId>
    <version>2.20.162</version>
</dependency>
```

### Test Dependencies

```xml
<dependency>org.junit.jupiter:junit-jupiter:5.10.2 (test)</dependency>
<dependency>org.mockito:mockito-core:4.11.0 (test)</dependency>
<dependency>org.awaitility:awaitility:4.3.0 (test)</dependency>
<dependency>org.assertj:assertj-core:3.24.2 (test)</dependency>
<dependency>org.testcontainers:testcontainers:1.20.4 (test)</dependency>
```

---

## Configuration Reference

### QueueProperty

| Property          | Type        | Default | Description                          |
|-------------------|-------------|---------|--------------------------------------|
| `fifo`            | `boolean`   | `false` | Whether the SQS queue is FIFO        |
| `queueUrl`        | `String`    | —       | The SQS queue URL                    |
| `maximumPoolSize` | `int`       | —       | Max threads for the producer pool    |
| `maxBatchSize`    | `int`       | —       | Max messages per batch request       |
| `linger`          | `long` (ms) | —       | Time to wait before flushing a batch |

**Note**: The in-memory buffer size = `maximumPoolSize × maxBatchSize`. Large values consume proportionally more memory. For **FIFO** queues, `maximumPoolSize` is forced to `1` and the `queueUrl` must end in `.fifo`.

---

## Usage Examples

### 1. Setup with Builder (Recommended)

```java
// For AWS SDK v1 — AmazonSQS client
// For AWS SDK v2 — SqsClient

QueueProperty queueProperty = QueueProperty.builder()
    .fifo(false)
    .linger(100L)
    .maxBatchSize(10)
    .maximumPoolSize(5)
    .queueUrl("http://localhost:4566/000000000000/queue")
    .build();

AmazonSqsTemplate<MyMessage> template = AmazonSqsTemplate.builder(sqsClient, queueProperty)
    .meterRegistry(new SimpleMeterRegistry())
    .queueRequests(new RingBufferBlockingQueue<>(1024))
    .build();
```

### 2. Sending a Standard Message

```java
template.send(
    RequestEntry.<MyMessage>builder()
        .withValue(new MyMessage("hello"))
        .withMessageHeaders(Map.of("source", "app-1"))
        .build()
);
```

### 3. Sending a FIFO Message

```java
template.send(
    RequestEntry.<MyMessage>builder()
        .withValue(new MyMessage("ordered-msg"))
        .withGroupId("my-group-id")
        .withDeduplicationId(UUID.randomUUID().toString())
        .build()
);
```

### 4. Async Callbacks

```java
template.send(requestEntry)
    .addCallback(
        success -> log.info("Sent: {} ({})", success.getMessageId(), success.getSequenceNumber()),
        failure -> log.error("Failed: {} [{}]", failure.getMessage(), failure.getCode())
    );
```

### 5. Await Completion and Shutdown

```java
template.send(requestEntry);
template.await().thenRun(template::shutdown).join();
```

### 6. Custom ObjectMapper and BlockingQueue

```java
AmazonSqsTemplate<MyMessage> template = new AmazonSqsTemplate<>(
    amazonSQS,
    queueProperty,
    new LinkedBlockingQueue<>(100),
    new ObjectMapper()
);
```

---

## Metrics (Micrometer)

The library integrates with Micrometer and records the following metrics when a `MeterRegistry` is provided via the builder's `.meterRegistry(registry)`.

### SQS Publish Metrics

Tags: `queue` = `<queueUrl>`

| Metric Name            | Type                  | Description                                        | Config                                   |
|------------------------|-----------------------|----------------------------------------------------|------------------------------------------|
| `sqs.publish.attempts` | `Counter`             | Total number of SQS SendMessageBatch calls attempted | —                                        |
| `sqs.publish.success`  | `Counter`             | Individual SQS messages acknowledged as successful | —                                        |
| `sqs.publish.failure`  | `Counter`             | Individual SQS messages that failed                | Dynamic tags: `error_code`, `error_type` |
| `sqs.publish.duration` | `Timer`               | End-to-end latency of SQS SendMessageBatch calls   | Percentiles: 0.5, 0.95, 0.99             |
| `sqs.publish.batch.size` | `DistributionSummary` | Number of entries per SQS SendMessageBatch request | —                                        |
| `sqs.publish.inflight` | `Gauge`               | SendMessageBatches currently in progress           | Backed by `AtomicInteger`                |

The `sqs.publish.failure` counter is created dynamically with additional `error_code` (AWS error code string) and `error_type` (amazon_service_exception or unknown) tags.

### Blocking Queue Metrics

Tags: `name` = `<queueName>`

| Metric Name                    | Type      | Description                                                          | Config                  |
|--------------------------------|-----------|----------------------------------------------------------------------|-------------------------|
| `blocking.queue.puts.total`    | `Counter` | Total number of successful put operations                            | —                       |
| `blocking.queue.puts.failed`   | `Counter` | Total number of put operations that threw an exception               | —                       |
| `blocking.queue.put.duration`  | `Timer`   | Latency of put operations (including wait time when queue is full)   | Percentile histogram    |
| `blocking.queue.takes.total`   | `Counter` | Total number of successful take operations                           | —                       |
| `blocking.queue.takes.failed`  | `Counter` | Total number of take operations that threw an exception              | —                       |
| `blocking.queue.take.duration` | `Timer`   | Latency of take operations (including wait time when queue is empty) | Percentile histogram    |
| `blocking.queue.size`          | `Gauge`   | Current number of elements in the queue                              | Calls `delegate.size()` |

### Executor Metrics

Tags: `name` = `<executorName>`

| Metric Name                | Type      | Description                                                        | Config                    |
|----------------------------|-----------|--------------------------------------------------------------------|---------------------------|
| `executor.active`          | `Gauge`   | Number of tasks currently being executed by pool threads           | Backed by `AtomicInteger` |
| `executor.tasks.succeeded` | `Counter` | Total number of tasks that completed without throwing an exception | —                         |
| `executor.tasks.failed`    | `Counter` | Total number of tasks that completed by throwing an exception      | —                         |
| `executor.task.duration`   | `Timer`   | Wall-clock duration of each task execution                         | —                         |

---

## Threading Model

- **Standard queues**: Uses `AmazonSqsThreadPoolExecutor` with `SynchronousQueue`, zero core threads, and `BlockingSubmissionPolicy` (30s blocking timeout). Threads are created on demand.
- **FIFO queues**: Single-threaded executor to guarantee message ordering.
- **Consumer scheduler**: A `ScheduledExecutorService` with a single daemon thread drains the queue at each `linger` interval.

---

## Exception Handling

| Exception                                                         | Condition                                            |
|-------------------------------------------------------------------|------------------------------------------------------|
| `MaximumAllowedMessageException`                                  | A single message exceeds the 1024KB SQS payload limit |
| SDK exceptions (`AmazonServiceException` / `AwsServiceException`) | Service-side errors during `sendMessageBatch`         |
| SDK exceptions (`AmazonClientException` / `AwsClientException`)   | Client-side errors (network, serialization)          |

Failed messages are delivered to the failure callback with:

- `messageId` — the original request ID
- `code` — the error code
- `message` — error description
- `senderFault` — whether the error is a client or server fault

---

## Testing

```bash
# Run unit tests
mvn test

# Run integration tests (requires Docker for Ministack)
mvn verify
```

The integration tests use [Testcontainers](https://testcontainers.com) with `ministackorg/ministack:1.4.0` to spin up a real SQS-compatible service. Messages are verified by sending to a queue and polling for delivery.
