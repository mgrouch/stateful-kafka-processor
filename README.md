# Stateful Kafka Streams Processor

This repository contains a Java 17, Maven, Spring Boot, multi-module project for a stateful Kafka Streams processor with explicit DB synchronization events.

## Modules

- `domain-model`: domain records `T`, `S`, `TS`
- `messaging-model`: `MessageEnvelope`, `DbSyncEnvelope`, and related enums
- `processor`: Kafka Streams topology, state stores, signed allocation logic, mutation emission
- `db-sync-app`: SQL writer that consumes `db-sync-events` and applies mutations transactionally

## Exact domain structure (`T`, `S`, `TS`)

The runtime contract follows the Java records exactly.

### `T`

```text
T(
  id, pid, ref, accId,
  tt, tDate, sDate, a_status, cancel,
  q, q_a_total, q_a_delta_last, q_f
)
```

Validation and defaults:
- `id`, `pid`, `ref` are non-null/non-blank.
- `accId` is optional but cannot be blank when present.
- `tt` defaults to `B` when null.
- `a_status` defaults to `NORM` when null.
- `q != 0`; sign enforced by `tt`:
  - `tt in {B, CS}` => `q > 0`
  - `tt in {S, SS}` => `q < 0`
- `q_a_total` sign must match `q` (or be 0), and `abs(q_a_total) <= abs(q)`.
- `q_a_delta_last` sign must match `q` when non-zero, and `abs(q_a_delta_last) <= abs(q)`.
- `a_status == FAIL` requires `q_f != 0`.

### `S`

```text
S(id, pid, bDate, q, q_carry, q_a, rollover, dir)
```

Validation and defaults:
- `id`, `pid` are non-null/non-blank.
- `dir` defaults from `q` when null (`q<0 => D`, `q>0 => R`).
- `q + q_carry != 0`.
- If both `q` and `q_carry` are non-zero, they must share sign.
- `dir == D => q < 0`; `dir == R => q > 0`.
- `q_a` sign must match `q + q_carry` (or be 0), and `abs(q_a) <= abs(q + q_carry)`.

### `TS`

```text
TS(id, pid, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt)
```

Validation and defaults:
- `id`, `pid`, `tid`, `sid` are non-null/non-blank.
- `accId` is optional but cannot be blank when present.
- `tt` defaults to `B` when null.
- `q != 0`.
- `q_a_delta` sign matches `q` and `abs(q_a_delta) <= abs(q)`.
- `q_a_total_after` sign matches `q` (or is 0) and `abs(q_a_total_after) <= abs(q)`.

## Runtime behavior

- Input topic: `input-events`
- Processed output topic: `processed-events`
- DB sync output topic: `db-sync-events`
- Kafka key remains `pid` for all topics.

Kafka Streams state stores:

- `unprocessed-t-store`
- `unprocessed-s-store`
- `t-dedupe-store`

## Allocation logic (exact flow)

Default runtime strategy is `AutoAllocOppositeStrategy` (the naive strategy remains available only for explicit wiring/tests).

### Shared math

- `remainingT = T.q - T.q_a_total`
- `remainingS = remainingCarry + remainingRegular`, where carry is consumed before regular quantity.
- Sign compatibility check: both residuals are non-zero and have identical sign.
- Allocation delta:
  - `delta = sign(targetResidual) * min(abs(targetResidual), abs(sourceResidual))`
  - if sign-incompatible => `delta = 0`

### Incoming `T`

1. Dedupe by key `pid|ref|cancel` with 14-day stream-time window.
2. Emit `ACCEPTED_T` if not deduped.
3. Load `S` candidates for the same `pid`.
4. Split by sign compatibility against incoming `T` residual.
5. Order only compatible `S` candidates by:
   - `rollover=true` first
   - then `id` ascending
6. Iterate ordered compatible candidates:
   - apply signed allocation
   - update `T.q_a_total += delta`
   - set `T.q_a_delta_last = delta`
   - update candidate `S.q_a += delta`
   - emit `TS(idPrefix-index, pid, tid, sid, accId, tDate, bDate, T.q, delta, T.q_a_total_after, tt)`
7. Append sign-incompatible `S` unchanged.
8. Persist every updated `S` with per-item upsert/delete mutation according to open/closed residual.
9. Persist incoming `T` with upsert/delete mutation according to open/closed residual.

Special `sDate` rule for `T`: when `T` becomes fully allocated (`q_a_total == q`) during an allocation step, set `T.sDate` to candidate `S.bDate` when present (otherwise keep current `sDate`).

### Incoming `S`

1. Emit `ACCEPTED_S`.
2. Load `T` candidates for the same `pid`.
3. Auto-allocate opposite-sign `T` first (for both previously ordered and untouched candidates):
   - for each opposite-sign `T`, force-close by setting `q_a_total = q`
   - set `q_a_delta_last = q - old(q_a_total)`
   - accumulate incoming `S.q_a_opposite_delta` and `S.q_a_opposite_total`
   - emit `TS(...)` for every non-zero opposite delta
4. Split remaining `T` by sign compatibility against the updated incoming `S` residual.
5. On compatible remaining `T` only:
   - canonical sort by `id`
   - bucket into `a_status == FAIL` and others
   - deterministic shuffle each bucket using seed hash of `(allocationLotterySeed, pid, incomingS.id, "INCOMING_S", bucketName)`
   - process FAIL bucket first, then NORM bucket
6. Iterate in that order:
   - apply signed allocation
   - update candidate `T.q_a_total += delta`
   - set candidate `T.q_a_delta_last = delta`
   - update incoming `S.q_a += delta`
   - emit `TS(...)` with `q = T.q`, `q_a_delta = delta`, `q_a_total_after = updated T.q_a_total`
7. Append sign-incompatible remaining `T` unchanged, then append already auto-allocated opposite-sign `T`.
8. Persist every updated `T` with per-item upsert/delete mutation according to open/closed residual.
9. Persist incoming `S` with upsert/delete mutation according to open/closed residual.

Special `sDate` rule for `T`: same as incoming `T` flow (set when a `T` becomes fully allocated).

### Incoming `TS`

- Forward to processed stream.
- Emit `GENERATED_TS` mutation.

## DB synchronization events

For each accepted/generated/store mutation, the processor emits `DbSyncEnvelope` records with:

- `mutationType`
- `pid`
- one payload among `T` / `S` / `TS` (or none for delete)
- source topic/partition/offset/timestamp
- per-input `ordinal`
- deterministic `eventId = topic-partition-offset-ordinal-mutationType`

Mutation types used:

- `ACCEPTED_T`
- `ACCEPTED_S`
- `GENERATED_TS`
- `UPSERT_UNPROCESSED_T`
- `UPSERT_UNPROCESSED_S`
- `DELETE_UNPROCESSED_T`
- `DELETE_UNPROCESSED_S`

## SQL copy writer

`db-sync-app` consumes `db-sync-events` and writes to SQL.

Key guarantees:

- auto-commit disabled
- records applied in partition order
- row mutations + consumer offset checkpoint committed in one SQL transaction
- restart resumes from SQL checkpoint table `kafka_consumer_offsets`

### Minimal SQL schema

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
