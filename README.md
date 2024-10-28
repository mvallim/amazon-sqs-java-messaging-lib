# Amazon SQS Java Messaging Lib

[![Java CI with Maven](https://github.com/mvallim/amazon-sqs-java-messaging-lib/actions/workflows/branch.yml/badge.svg)](https://github.com/mvallim/amazon-sqs-java-messaging-lib/actions/workflows/branch.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=amazon-sqs-java-messaging-lib&metric=alert_status)](https://sonarcloud.io/dashboard?id=amazon-sqs-java-messaging-lib)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=amazon-sqs-java-messaging-lib&metric=coverage)](https://sonarcloud.io/dashboard?id=amazon-sqs-java-messaging-lib)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.mvallim/amazon-sqs-java-messaging-lib/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.mvallim/amazon-sqs-java-messaging-lib)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

The Amazon SQS Java Messaging Library holds the compatible classes, that are used for communicating with Amazon Simple Queue Service. This project builds on top of the AWS SDK for Java to use Amazon SQS provider for the messaging applications without running any additional software.

> The batch size should be chosen based on the size of individual messages and available network bandwidth as well as the observed latency and throughput improvements based on the real life load. These are configured to some sensible defaults assuming smaller message sizes and the optimal batch size for server side processing.

## Request Batch

Combine multiple requests to optimally utilise the network.

Article [Martin Fowler](https://martinfowler.com) [Request Batch](https://martinfowler.com/articles/patterns-of-distributed-systems/request-batch.html)

_**Compatible JDK 8, 11 and 17**_

_**Compatible AWS JDK v1 >= 1.12**_

_**Compatible AWS JDK v2 >= 2.18**_

This library supports **`Kotlin`** aswell

## 1. Quick Start

### 1.1 Prerequisite

In order to use Amazon SQS Java Messaging Lib within a Maven project, simply add the following dependency to your pom.xml. There are no other dependencies for Amazon SQS Java Messaging Lib, which means other unwanted libraries will not overwhelm your project.

You can pull it from the central Maven repositories:

#### Maven

##### For AWS SDK v1

```xml
<dependency>
    <groupId>com.github.mvallim</groupId>
    <artifactId>amazon-sqs-java-messaging-lib-v1</artifactId>
    <version>1.0.0</version>
</dependency>
```

##### For AWS SDK v2

```xml
<dependency>
    <groupId>com.github.mvallim</groupId>
    <artifactId>amazon-sqs-java-messaging-lib-v2</artifactId>
    <version>1.0.0</version>
</dependency>
```

If you want to try a snapshot version, add the following repository:

```xml
<repository>
    <id>sonatype-snapshots</id>
    <name>Sonatype Snapshots</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

#### Gradle

##### For AWS SDK v1

```groovy
implementation 'com.github.mvallim:amazon-sqs-java-messaging-lib-v1:1.0.0'
```

##### For AWS SDK v2

```groovy
implementation 'com.github.mvallim:amazon-sqs-java-messaging-lib-v2:1.0.0'
```

If you want to try a snapshot version, add the following repository:

```groovy
repositories {
  maven {
    url "https://oss.sonatype.org/content/repositories/snapshots"
  }
}
```

## 1.2 Usage

### Properties `QueueProperty`

| Property              | Type        | Description                                                                    |
|-----------------------|-------------|--------------------------------------------------------------------------------|
| **`fifo`**            | **boolean** | refers if SQS is fifo or not.                                                  |
| **`maximumPoolSize`** | **int**     | refers maximum threads for producer.                                           |
| **`queueUrl`**        | **string**  | refers queue url.                                                              |
| **`linger`**          | **int**     | refers to the time to wait before sending messages out to SQS.                 |
| **`maxBatchSize`**    | **int**     | refers to the maximum amount of data to be collected before sending the batch. |

**NOTICE**: the buffer of message store in memory is calculate using **`maximumPoolSize`** * **`maxBatchSize`** huge values demand huge memory.

#### Determining the type of `BlockingQueue` with its maximum capacity

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(false)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();
  
final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(
  amazonSqS, QueueProperty, new LinkedBlockingQueue<>(100));
```

#### Using an `ObjectMapper` other than the default

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(false)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();
  
final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(
  amazonSqS, queueProperty, new ObjectMapper<>());
```

#### Using an `ObjectMapper` and a `BlockingQueue` other than the default

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(false)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();
  
final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(
  amazonSqS, queueProperty, new LinkedBlockingQueue<>(100), new ObjectMapper<>());
```

### Standard SQS

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(false)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();

final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(amazonSqS, queueProperty);

final RequestEntry<MyMessage> requestEntry = RequestEntry.builder()
  .withValue(new MyMessage())
  .withMessageHeaders(Map.of())
  .build();

sqsTemplate.send(requestEntry);
```

### FIFO SQS

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(true)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();

final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(amazonSqS, queueProperty);

final RequestEntry<MyMessage> requestEntry = RequestEntry.builder()
  .withValue(new MyMessage())
  .withMessageHeaders(Map.of())
  .withGroupId(UUID.randomUUID().toString())
  .withDeduplicationId(UUID.randomUUID().toString())
  .build();

sqsTemplate.send(requestEntry);
```

### Send With Callback

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(true)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();

final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(amazonSqS, queueProperty);

final RequestEntry<MyMessage> requestEntry = RequestEntry.builder()
  .withValue(new MyMessage())
  .withMessageHeaders(Map.of())
  .withGroupId(UUID.randomUUID().toString())
  .withDeduplicationId(UUID.randomUUID().toString())
  .build();

sqsTemplate.send(requestEntry).addCallback(result -> {
  successCallback -> LOGGER.info("{}", successCallback), 
  failureCallback -> LOGGER.error("{}", failureCallback)
});

sqsTemplate.send(requestEntry).addCallback(result -> {
  successCallback -> LOGGER.info("{}", successCallback)
});
```

### Send And Wait

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(true)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();

final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(amazonSqS, queueProperty);

final RequestEntry<MyMessage> requestEntry = RequestEntry.builder()
  .withValue(new MyMessage())
  .withMessageHeaders(Map.of())
  .withGroupId(UUID.randomUUID().toString())
  .withDeduplicationId(UUID.randomUUID().toString())
  .build();

sqsTemplate.send(requestEntry).addCallback(result -> {
  successCallback -> LOGGER.info("{}", successCallback), 
  failureCallback -> LOGGER.error("{}", failureCallback)
});

sqsTemplate.await().join();
```

### Send And Shutdown

```java
final QueueProperty queueProperty = QueueProperty.builder()
  .fifo(true)
  .linger(100)
  .maxBatchSize(10)
  .maximumPoolSize(20)
  .queueUrl("http://localhost:4566/000000000000/queue")
  .build();

final AmazonSqsTemplate<MyMessage> sqsTemplate = new AmazonSqsTemplate<>(amazonSqS, queueProperty);

final RequestEntry<MyMessage> requestEntry = RequestEntry.builder()
  .withValue(new MyMessage())
  .withMessageHeaders(Map.of())
  .withGroupId(UUID.randomUUID().toString())
  .withDeduplicationId(UUID.randomUUID().toString())
  .build();

sqsTemplate.send(requestEntry).addCallback(result -> {
  successCallback -> LOGGER.info("{}", successCallback), 
  failureCallback -> LOGGER.error("{}", failureCallback)
});

sqsTemplate.shutdown();
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [GitHub](https://github.com/mvallim/amazon-sqs-java-messaging-lib) for versioning. For the versions available, see the [tags on this repository](https://github.com/mvallim/amazon-sqs-java-messaging-lib/tags).

## Authors

* **Marcos Vallim** - _Founder, Author, Development, Test, Documentation_ - [mvallim](https://github.com/mvallim)

See also the list of [contributors](CONTRIBUTORS.txt) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details
