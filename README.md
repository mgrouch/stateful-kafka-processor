# Stateful Kafka Streams Processor

This repository contains a Java 17, Maven, Spring Boot, multi-module project for a stateful Kafka Streams processor with explicit DB synchronization events.

## Modules

- `domain-model`: domain records `T`, `S`, `TS`
- `messaging-model`: `MessageEnvelope`, `DbSyncEnvelope`, and related enums
- `processor`: Kafka Streams topology, state stores, DB sync writer, SQL schema, tests

## Runtime behavior

- Input topic: `input-events`
- Processed output topic: `processed-events`
- DB sync output topic: `db-sync-events`
- Kafka key remains `pid` for all topics.

Kafka Streams state stores are kept only for processing:

- `unprocessed-t-store`
- `unprocessed-s-store`
- `t-dedupe-store`

No `processedT`/`processedS` stores are introduced.

### Allocation sign behavior

- Incoming `T` allocates only against `S` with sign-compatible remaining supply.
- Incoming `S` has a mandatory direction/sign constraint: `dir=D => q<0`, `dir=R => q>0`.
- Incoming `S` first force-closes opposite-side `T` and emits `TS` for each full close:
  - `S.dir=R`: every matching `T` with `q<0` is fully allocated (`T.q_a_total = T.q`).
  - `S.dir=D`: every matching `T` with `q>0` is fully allocated (`T.q_a_total = T.q`).
- After forced closes, remaining `S` quantity allocates against sign-compatible `T` with normal priority/lottery ordering.

### Allocation quantity fields

- `T.q_a_total`: source-of-truth total allocated so far on `T`.
- `T.q_a_delta_last`: last allocation delta applied to `T` (informational/stateful, not used as close/open truth).
- `TS.q_a_delta`: per-event allocation delta represented by the `TS` event.
- `TS.q_a_total_after`: resulting `T.q_a_total` after the `TS` delta is applied.

Population rule for each allocation step with delta `D`:

- `previousTotal = T.q_a_total`
- `newTotal = previousTotal + D`
- update `T.q_a_total = newTotal`
- update `T.q_a_delta_last = D`
- emit `TS.q_a_delta = D`
- emit `TS.q_a_total_after = newTotal`

Signed quantities are preserved for these fields (`q_a_total`, `q_a_delta_last`, `q_a_delta`, `q_a_total_after`), including negative values.

### DB synchronization events

For each accepted/generated mutation, the processor emits explicit `DbSyncEnvelope` records to `db-sync-events` with deterministic metadata:

- mutation type
- `pid`
- payload (`T`, `S`, `TS`, or none for deletes)
- source topic/partition/offset/timestamp
- per-input ordinal
- deterministic `eventId`

Current mappings:

- accepted `T` emits: `ACCEPTED_T`, `UPSERT_UNPROCESSED_T`, `GENERATED_TS`
- accepted `S` emits: `ACCEPTED_S`, `UPSERT_UNPROCESSED_S`
- forwarded `TS` emits: `GENERATED_TS`

## SQL copy writer

`DbSyncWriter` is a separate component that consumes `db-sync-events` and writes to SQL.

Key guarantees:

- auto-commit is disabled
- records are applied in partition order
- row mutations + offset checkpoint are committed in the **same SQL transaction**
- restart uses offsets from SQL table `kafka_consumer_offsets`

### Minimal SQL schema

The writer creates/uses the following tables:

- `accepted_t`
- `accepted_s`
- `generated_ts`
- `unprocessed_t_state`
- `unprocessed_s_state`
- `kafka_consumer_offsets`

## Build

```bash
mvn clean verify
```

## Run locally

```bash
docker compose up -d kafka
mvn -pl processor spring-boot:run -Dspring-boot.run.arguments="--spring.kafka.bootstrap-servers=localhost:9093 --app.kafka.security-protocol=SSL --app.kafka.ssl.truststore-location=/path/to/client.truststore.jks --app.kafka.ssl.truststore-password=changeit --app.application-id=stateful-data-processor --app.input-topic=input-events --app.output-topic=processed-events --app.db-sync-topic=db-sync-events"
```
