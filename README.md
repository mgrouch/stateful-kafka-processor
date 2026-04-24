# Stateful Kafka Streams Processor

This repository contains a Java 17, Maven, Spring Boot, multi-module project for a **DSF-style stateful data processor** built on **Kafka Streams**.

## Modules

- `domain-model`: domain records `T`, `S`, `TS`
- `messaging-model`: `MessageEnvelope` and `MessageKind`
- `processor`: Spring Boot application, Kafka Streams topology, state stores, tests, Docker Compose, and CI

## What it does today

- Reads `MessageEnvelope` records from topic `input-events`
- Uses `pid` as the logical partition key for state and output. Producers should publish to `input-events` with Kafka record key = `pid` for strict per-`pid` ordering.
- Keeps three persistent Kafka Streams state stores:
  - `unprocessed-t-store`
  - `unprocessed-s-store`
  - `t-dedupe-store` (deduplication index for `T` events)
- On `T` input:
  - stores the `T` in `unprocessed-t-store`
  - emits a derived `TS` to `processed-events`
- On `S` input:
  - stores the `S` in `unprocessed-s-store`
- On `TS` input:
  - forwards the `TS` envelope to `processed-events`

## Why Kafka Streams is a good fit

Yes, this is a good fit for Kafka Streams for the workflow you described:

- partition-by-`pid` processing is natural because Kafka preserves order within a partition
- local state stores plus changelog topics give you crash recovery and state restoration
- exactly-once read-process-write is supported through `exactly_once_v2`
- later matching logic between `T` and `S` can stay in the same stateful processor
- later SQL copy is typically done with a separate consumer, Kafka Connect JDBC sink, or another downstream processor

## Build

From the repository root:

```bash
mvn clean verify
```

Run integration tests against Kafka:

```bash
docker compose up -d kafka
mvn verify -Prun-kafka-it
```

## Run locally

Start Kafka:

```bash
docker compose up -d kafka
```

Run the processor:

```bash
mvn -pl processor spring-boot:run   -Dspring-boot.run.arguments="--spring.kafka.bootstrap-servers=localhost:9092 --app.application-id=stateful-data-processor --app.input-topic=input-events --app.output-topic=processed-events"
```

## Topics

Create topics manually if needed:

```bash
docker exec -it $(docker compose ps -q kafka) /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --create --topic input-events --partitions 3 --replication-factor 1

docker exec -it $(docker compose ps -q kafka) /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --create --topic processed-events --partitions 3 --replication-factor 1
```

## Message format

Example `T` envelope:

```json
{
  "kind": "T",
  "t": {
    "id": "t-100",
    "pid": "IBM",
    "q": 1000
  }
}
```

Example `S` envelope:

```json
{
  "kind": "S",
  "s": {
    "id": "s-100",
    "pid": "IBM",
    "q": 600
  }
}
```

Example emitted `TS` envelope:

```json
{
  "kind": "TS",
  "ts": {
    "id": "ts-t-100",
    "pid": "IBM",
    "q": 1000
  }
}
```

## T deduplication logic

The processor deduplicates incoming `T` events before writing state and emitting `TS`:

- dedupe key: `pid|ref|cancel`
- dedupe store: `t-dedupe-store` (value = event timestamp in milliseconds)
- dedupe window: 14 days (`DEDUPE_WINDOW_MILLIS`)
- behavior: if the same key is seen again within 14 days, the duplicate `T` is skipped (no state append and no output event)
- first-seen (or outside-window) events are processed normally: the key timestamp is updated, `T` is appended to `unprocessed-t-store`, and a derived `TS` is emitted
- cleanup: every 1 hour of stream-time punctuation, entries older than the 14-day window are evicted from `t-dedupe-store`

This keeps dedupe memory bounded while preserving idempotent processing for repeated `T` messages over the configured window.

## Exactly-once and ordering

This project configures Kafka Streams with:

- `processing.guarantee=exactly_once_v2`
- `num.stream.threads=1`
- persistent RocksDB-backed state stores

Ordering is guaranteed **within each partition**, which is the normal Kafka model. To preserve strict per-`pid` ordering on the input side, producers should write to `input-events` with Kafka record key = `pid`. The processor writes output with key = `pid`.

## Later SQL copy

When you are ready to copy processor state and processed events into SQL with delay, the normal approach is to keep the Kafka Streams processor unchanged and add one of these downstream patterns:

- Kafka Connect JDBC sink for `processed-events`
- a dedicated consumer that writes to SQL
- a compacted changelog topic or dedicated snapshot topic for derived state, then sink that to SQL
