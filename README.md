# Stateful Kafka Streams Processor

Java 17 + Spring Boot + Kafka Streams multi-module project with an explicit Kafka-driven SQL synchronization contract.

## Modules

- `domain-model`: domain records `T`, `S`, `TS`
- `messaging-model`: `MessageEnvelope`, `DbSyncEnvelope`, and mutation enums
- `processor`: Kafka Streams topology, state stores, and SQL db-sync writer

## Runtime behavior

Input topic: `input-events` (key = `pid`)  
Output topics:
- `processed-events` (business output)
- `db-sync-events` (explicit DB mutation stream)

State stores remain processing-only:
- `unprocessed-t-store`
- `unprocessed-s-store`
- `t-dedupe-store`

No `processedT` / `processedS` stores are introduced.

### Per-input mutation emission

For accepted `T` input:
1. `ACCEPTED_T`
2. `UPSERT_UNPROCESSED_T`
3. `GENERATED_TS`

For accepted `S` input:
1. `ACCEPTED_S`
2. `UPSERT_UNPROCESSED_S`

Each `DbSyncEnvelope` carries:
- mutation type
- `pid`
- payload (`T` / `S` / `TS`)
- source topic / partition / offset / timestamp
- per-input ordinal
- deterministic `eventId`

## Why `db-sync-events` instead of Kafka Streams internal changelog topics

Internal changelog topics are implementation details of Kafka Streams state stores. They are not a stable, user-owned contract for external systems.

`db-sync-events` is explicit and domain-level:
- owned by your application
- evolvable schema
- deterministic mutation semantics for DB writes
- decoupled from store internals and RocksDB/changelog mechanics

## Kafka-side atomicity

Kafka Streams is still configured with `processing.guarantee=exactly_once_v2`.

For each consumed input record, the topology atomically commits:
- state store/changelog updates
- `processed-events` writes
- `db-sync-events` writes
- input offset progression

That preserves the original Kafka-side exactly-once read-process-write guarantees.

## DB sync writer design

`DbSyncWriter` is a separate plain Kafka consumer (auto-commit disabled) that consumes `db-sync-events` in partition order and writes into SQL.

Pattern:
1. Poll db-sync records.
2. Group records by original source input (`sourceTopic/sourcePartition/sourceOffset`).
3. Open SQL transaction.
4. Apply all grouped row mutations.
5. Upsert consumed Kafka offset (`kafka_consumer_offsets`) in the same SQL transaction.
6. Commit.

On restart, the writer:
- reads offsets from `kafka_consumer_offsets`
- assigns topic partitions
- seeks to DB-stored offsets

This makes DB mutations and consumption position move atomically together.

## Minimal SQL schema

Managed by `JdbcDbSyncSqlRepository.initializeSchema()`:
- `accepted_t`
- `accepted_s`
- `generated_ts`
- `unprocessed_t_state`
- `unprocessed_s_state`
- `kafka_consumer_offsets`
- `db_sync_applied_events` (idempotency ledger)

## Guarantees and non-guarantees

### Guarantees
- Per Kafka partition / per `pid` ordering is preserved.
- For one input event, all related DB mutations commit together with offset checkpoint.
- DB may lag Kafka but will converge consistently.

### Non-guarantees
- No global total order across partitions.
- No cross-partition atomic transaction semantics.

## Build and test

```bash
mvn clean verify
```

## Run processor

```bash
mvn -pl processor spring-boot:run \
  -Dspring-boot.run.arguments="--spring.kafka.bootstrap-servers=localhost:9092 --app.application-id=stateful-data-processor --app.input-topic=input-events --app.output-topic=processed-events --app.db-sync-topic=db-sync-events"
```

## Optional DB writer runtime flags

- `--app.db-writer.enabled=true`
- `--app.db-writer.consumer-group=db-sync-writer`
- `--app.db-writer.poll-timeout-ms=500`

Also configure a JDBC datasource (`spring.datasource.*`) for your SQL database.
