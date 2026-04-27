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
  id, pid, pidAlt1, pidAlt2, ref, accId, sorId, oarId,
  tt, tDate, sDate,
  tCycle, sMode, a_status, activity, mStatus,
  cancel,
  q, q_a_total, q_a_delta_last, q_f,
  ledgerTime
)
```

Validation and defaults:
- Required text: `id`, `pid`, `ref`.
- Optional text (if present, must be non-blank): `pidAlt1`, `pidAlt2`, `accId`, `sorId`, `oarId`.
- Defaults: `tt=B`, `tCycle=SD`, `sMode=CN`, `a_status=NORM`, `activity=A01`, `mStatus=U`.
- `q != 0`; sign enforced by `tt`:
  - `tt in {B, CS}` => `q > 0`
  - `tt in {S, SS}` => `q < 0`
- `q_a_total` sign must match `q` (or be 0), and `abs(q_a_total) <= abs(q)`.
- `q_a_delta_last` sign must match `q` when non-zero, and `abs(q_a_delta_last) <= abs(q)`.
- `a_status == FAIL` requires `q_f != 0`.

### `S`

```text
S(
  id, pid, bDate,
  q, q_carry, q_a, q_unalloc, q_unkn,
  q_a_opposite_delta, q_a_opposite_total,
  rollover, o,
  dir, sCycle,
  ledgerTime
)
```

Validation and defaults:
- Required text: `id`, `pid`.
- Defaults: `dir` inferred from `q` (`q<0 => D`, `q>0 => R`), `sCycle=SD`.
- `q + q_carry != 0`.
- If both `q` and `q_carry` are non-zero, they must share sign.
- `dir == D => q < 0`; `dir == R => q > 0`.
- `q_a` sign must match `q + q_carry` (or be 0), and `abs(q_a) <= abs(q + q_carry)`.

### `TS`

```text
TS(
  id, pid, pidAlt1, pidAlt2,
  tid, sid,
  accId, sorId, oarId,
  tDate, sDate,
  q, q_a_delta, q_a_total_after,
  tt, activity, mStatus,
  o, cancel
)
```

Validation and defaults:
- Required text: `id`, `pid`, `tid`, `sid`.
- Optional text (if present, must be non-blank): `pidAlt1`, `pidAlt2`, `accId`, `sorId`, `oarId`.
- Defaults: `tt=B`, `activity=A01`, `mStatus=U`.
- `q != 0`.
- `q_a_delta` sign matches `q`, and `abs(q_a_delta) <= abs(q)`.
- `q_a_total_after` sign matches `q` (or is 0), and `abs(q_a_total_after) <= abs(q)`.

## Runtime behavior

- Input topic: `input-events`
- Processed output topic: `processed-events`
- DB sync output topic: `db-sync-events`
- Failed `T` output topic: `failed-t-events`
- `S` with `q_carry` output topic: `s-with-q-carry-events`
- Recon report output topic: `recon-report-events`
- Kafka key remains `pid` for all sinks (input key mismatch only logs a warning).

Kafka Streams state stores:

- `unprocessed-t-store`
- `unprocessed-s-store`
- `t-dedupe-store`

## Allocation logic (exact flow)

Default runtime strategy is `AutoAllocOppositeStrategy` (naive remains available for explicit wiring/tests).

### Shared math

- `remainingT = T.q - T.q_a_total`
- `remainingS = remainingCarry + remainingRegular`, where carry is consumed first.
- Sign compatibility check: both residuals non-zero and same sign.
- Allocation delta:
  - `delta = sign(targetResidual) * min(abs(targetResidual), abs(sourceResidual))`
  - if sign-incompatible => `delta = 0`

### Incoming `T`

1. Dedupe by key `pid|ref|cancel` with 14-day stream-time window.
2. Emit `ACCEPTED_T` if not deduped.
3. If `a_status == FAIL`, also emit the `T` to `failed-t-events`.
4. Cancellation fast-path: if incoming `cancel=true` and an open in-store `T` exists with same `id` and `q_a_total==0`, force-close it, emit a cancel `TS`, and delete the open `T` state.
5. Otherwise load `S` candidates for same `pid`.
6. Keep only ledger-compatible + sign-compatible candidates for allocation (`T.ledgerTime <= S.ledgerTime` when both present).
7. Order compatible candidates: `rollover=true` first, then `id` ascending.
8. Iterate ordered candidates with signed allocation:
   - `T.q_a_total += delta`
   - `T.q_a_delta_last = delta`
   - `S.q_a += delta`
   - emit `TS(idPrefix-index, pid, pidAlt1, pidAlt2, tid, sid, accId, sorId, oarId, tDate, bDate, q=T.q, q_a_delta=delta, q_a_total_after, tt, o=S.o)`
9. Non-allocated candidates (sign/ledger-incompatible + untouched) are appended unchanged.
10. Persist every updated `S` with per-item upsert/delete mutation by open/closed residual.
11. Persist incoming `T` with upsert/delete mutation by open/closed residual.

Special `sDate` rule for `T`: when a `T` becomes fully allocated (`q_a_total == q`) during an allocation step, set `T.sDate` to candidate `S.bDate` when present; otherwise keep current `sDate`.

### Incoming `S`

1. Emit `ACCEPTED_S`.
2. If `q_carry != 0`, also emit to `s-with-q-carry-events`.
3. Load `T` candidates for same `pid`.
4. Auto-allocate opposite-sign `T` first over both ordered and untouched pools, but only when ledger-compatible:
   - set `T.q_a_total = T.q`
   - set `T.q_a_delta_last = T.q - old(T.q_a_total)`
   - emit `TS(...)` for non-zero opposite delta
   - accumulate into incoming `S.q_a_opposite_delta` and `S.q_a_opposite_total`
5. On remaining candidates, keep only ledger-compatible + sign-compatible ones using updated incoming `S` residual.
6. Canonical sort by `id`, then bucket by exact strategy predicates:
   - `PARTIAL_FAIL_S`, `PARTIAL_FAIL_NON_S`, `FULL_FAIL_S`, `FULL_FAIL_NON_S`, `PARTIAL_ALLOC_S`, `PARTIAL_ALLOC_NON_S`, `OPEN_S`, `OPEN_OTHER`
7. In each bucket:
   - split non-RT and RT (`tCycle == RT`)
   - deterministically shuffle non-RT using hash of `(allocationLotterySeed, pid, incomingS.id, "INCOMING_S", bucketName)`
   - append RT in FIFO order by `(ledgerTime nulls last, id)`
8. Process buckets in the exact order listed in step 6, applying signed allocation:
   - candidate `T.q_a_total += delta`
   - candidate `T.q_a_delta_last = delta`
   - incoming `S.q_a += delta`
   - emit `TS(...)` with `q = T.q`, `q_a_delta = delta`, `q_a_total_after = updated T.q_a_total`
9. Append sign/ledger-incompatible remaining `T` unchanged, then append the already auto-allocated opposite-sign `T`.
10. Persist every updated `T` with per-item upsert/delete mutation by open/closed residual.
11. Persist incoming `S` with upsert/delete mutation by open/closed residual.

Special `sDate` rule for `T`: same as incoming `T` flow.

### Incoming `TS`

- Forward to `processed-events` unchanged.
- Emit `GENERATED_TS` mutation.

## Topology and processor notes

- Topology has one source (`input-events-source`) and one processor (`stateful-envelope-processor`).
- Five sinks are wired from the processor: processed, db-sync, failed-T, S-with-carry, and recon-report.
- All three state stores are persistent key-value stores with changelog logging enabled.
- DB-sync metadata (`sourceTopic`, `sourcePartition`, `sourceOffset`) is required from Kafka record metadata.
- `eventId` is deterministic: `topic-partition-offset-ordinal-mutationType`.
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
mvn -pl processor spring-boot:run -Dspring-boot.run.arguments="--spring.kafka.bootstrap-servers=localhost:9093 --app.kafka.security-protocol=SSL --app.kafka.ssl.truststore-location=/path/to/client.truststore.jks --app.kafka.ssl.truststore-password=changeit --app.application-id=stateful-data-processor --app.input-topic=input-events --app.output-topic=processed-events --app.db-sync-topic=db-sync-events --app.failed-t-topic=failed-t-events --app.s-with-q-carry-topic=s-with-q-carry-events --app.recon-report-topic=recon-report-events"
```
