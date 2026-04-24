package com.example.stateful.processor.topology.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.StateStores;
import com.example.stateful.processor.state.TBucket;
import com.example.stateful.processor.topology.TopologyFactory;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class StatefulEnvelopeProcessor extends ContextualProcessor<String, MessageEnvelope, String, Object> {

    private static final Logger log = LoggerFactory.getLogger(StatefulEnvelopeProcessor.class);

    static final long DEDUPE_WINDOW_MILLIS = Duration.ofDays(14).toMillis();
    private static final Duration DEDUPE_CLEANUP_INTERVAL = Duration.ofHours(1);

    private KeyValueStore<String, TBucket> tStore;
    private KeyValueStore<String, SBucket> sStore;
    private KeyValueStore<String, Long> tDedupeStore;

    @Override
    public void init(ProcessorContext<String, Object> context) {
        super.init(context);
        this.tStore = context.getStateStore(StateStores.UNPROCESSED_T_STORE);
        this.sStore = context.getStateStore(StateStores.UNPROCESSED_S_STORE);
        this.tDedupeStore = context.getStateStore(StateStores.T_DEDUPE_STORE);

        context.schedule(DEDUPE_CLEANUP_INTERVAL, PunctuationType.STREAM_TIME, this::evictExpiredDedupeKeys);
    }

    @Override
    public void process(Record<String, MessageEnvelope> record) {
        if (record.value() == null) {
            return;
        }

        String pid = record.value().partitionKey();
        if (record.key() != null && !pid.equals(record.key())) {
            log.warn("Incoming record key {} does not match pid {}. For strict per-pid partition ordering, producers should publish with key=pid.", record.key(), pid);
        }

        int ordinal = 0;
        switch (record.value().kind()) {
            case T -> handleT(record, pid, ordinal);
            case S -> handleS(record, pid, ordinal);
            case TS -> handleTs(record, pid, ordinal);
            default -> throw new IllegalStateException("Unsupported kind " + record.value().kind());
        }
    }

    private void handleT(Record<String, MessageEnvelope> record, String pid, int ordinal) {
        T incomingT = record.value().t();
        long timestamp = eventTimestamp(record);
        String dedupeKey = buildDedupeKey(incomingT);
        Long seenAt = tDedupeStore.get(dedupeKey);

        if (seenAt != null && timestamp - seenAt <= DEDUPE_WINDOW_MILLIS) {
            log.info("Skipping duplicate T id={} pid={} dedupeKey={} seenAt={} currentTs={}", incomingT.id(), pid, dedupeKey, seenAt, timestamp);
            return;
        }

        tDedupeStore.put(dedupeKey, timestamp);
        emitDbSync(pid, DbSyncMutationType.ACCEPTED_T, incomingT, null, null, record, ordinal++);

        List<S> candidates = loadS(pid);
        AllocationResult allocation = allocateForIncomingT(incomingT, candidates, buildTsIdPrefix(record, "t", incomingT.id()));

        for (TS ts : allocation.emittedTs()) {
            emitProcessed(pid, MessageEnvelope.forTS(ts), record.timestamp());
            emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, ts, record, ordinal++);
        }

        persistUpdatedS(pid, allocation.updatedS(), record, new OrdinalRef(ordinal));
        ordinal = ordinal + allocation.updatedS().size();

        if (isOpen(allocation.updatedIncomingT())) {
            upsertT(pid, allocation.updatedIncomingT());
            emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, allocation.updatedIncomingT(), null, null, record, ordinal);
        } else {
            removeT(pid, allocation.updatedIncomingT().id());
            emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, null, null, null, record, ordinal);
        }

        log.info("Processed T id={} pid={} q={} q_a={}", allocation.updatedIncomingT().id(), pid, allocation.updatedIncomingT().q(), allocation.updatedIncomingT().q_a());
    }

    private void handleS(Record<String, MessageEnvelope> record, String pid, int ordinal) {
        S incomingS = record.value().s();
        emitDbSync(pid, DbSyncMutationType.ACCEPTED_S, null, incomingS, null, record, ordinal++);

        List<T> candidates = loadT(pid);
        AllocationResult allocation = allocateForIncomingS(candidates, incomingS, buildTsIdPrefix(record, "s", incomingS.id()));

        for (TS ts : allocation.emittedTs()) {
            emitProcessed(pid, MessageEnvelope.forTS(ts), record.timestamp());
            emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, ts, record, ordinal++);
        }

        persistUpdatedT(pid, allocation.updatedT(), record, new OrdinalRef(ordinal));
        ordinal = ordinal + allocation.updatedT().size();

        if (isOpen(allocation.updatedIncomingS())) {
            upsertS(pid, allocation.updatedIncomingS());
            emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, allocation.updatedIncomingS(), null, record, ordinal);
        } else {
            removeS(pid, allocation.updatedIncomingS().id());
            emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_S, null, null, null, record, ordinal);
        }

        log.info("Processed S id={} pid={} q={} q_a={}", allocation.updatedIncomingS().id(), pid, allocation.updatedIncomingS().q(), allocation.updatedIncomingS().q_a());
    }

    private void handleTs(Record<String, MessageEnvelope> record, String pid, int ordinalStart) {
        log.info("Forwarding TS id={} pid={}", record.value().ts().id(), pid);
        emitProcessed(pid, record.value(), record.timestamp());
        emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, record.value().ts(), record, ordinalStart);
    }

    private AllocationResult allocateForIncomingT(T incomingT, List<S> candidates, String idPrefix) {
        requireSamePid(candidates.stream().map(S::pid).toList(), incomingT.pid(), "S candidate");

        List<S> updatedS = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        T updatedT = incomingT;
        int tsIndex = 0;

        for (S candidate : candidates) {
            long tRemaining = remaining(updatedT.q(), updatedT.q_a());
            long sRemaining = remaining(candidate.q(), candidate.q_a());
            long allocated = Math.min(tRemaining, sRemaining);

            if (allocated > 0) {
                updatedT = new T(updatedT.id(), updatedT.pid(), updatedT.ref(), updatedT.cancel(), updatedT.q(), updatedT.q_a() + allocated);
                S nextS = new S(candidate.id(), candidate.pid(), candidate.q(), candidate.q_a() + allocated);
                updatedS.add(nextS);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedT.pid(), updatedT.id(), nextS.id(), updatedT.q(), allocated));
            } else {
                updatedS.add(candidate);
            }
        }

        validateAllocationOutput(updatedT.pid(), List.of(updatedT), updatedS, emitted);
        return new AllocationResult(updatedT, updatedS, emitted);
    }

    private AllocationResult allocateForIncomingS(List<T> candidates, S incomingS, String idPrefix) {
        requireSamePid(candidates.stream().map(T::pid).toList(), incomingS.pid(), "T candidate");

        List<T> updatedT = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        S updatedS = incomingS;
        int tsIndex = 0;

        for (T candidate : candidates) {
            long sRemaining = remaining(updatedS.q(), updatedS.q_a());
            long tRemaining = remaining(candidate.q(), candidate.q_a());
            long allocated = Math.min(tRemaining, sRemaining);

            if (allocated > 0) {
                T nextT = new T(candidate.id(), candidate.pid(), candidate.ref(), candidate.cancel(), candidate.q(), candidate.q_a() + allocated);
                updatedS = new S(updatedS.id(), updatedS.pid(), updatedS.q(), updatedS.q_a() + allocated);
                updatedT.add(nextT);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedS.pid(), nextT.id(), updatedS.id(), nextT.q(), allocated));
            } else {
                updatedT.add(candidate);
            }
        }

        validateAllocationOutput(updatedS.pid(), updatedT, List.of(updatedS), emitted);
        return new AllocationResult(null, List.of(), updatedS, updatedT, emitted);
    }

    private void validateAllocationOutput(String pid, List<T> allowedT, List<S> allowedS, List<TS> emittedTs) {
        Set<String> allowedTid = new HashSet<>();
        Set<String> allowedSid = new HashSet<>();
        java.util.Map<String, Long> tQuantityById = new java.util.HashMap<>();
        java.util.Map<String, Long> allocatedByTid = new java.util.HashMap<>();
        for (T t : allowedT) {
            if (!pid.equals(t.pid())) {
                throw new IllegalStateException("allocate output has T with different pid");
            }
            if (t.q_a() < 0 || t.q_a() > t.q()) {
                throw new IllegalStateException("allocate output has T q_a outside [0,q]");
            }
            allowedTid.add(t.id());
            tQuantityById.put(t.id(), t.q());
        }
        for (S s : allowedS) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("allocate output has S with different pid");
            }
            if (s.q_a() < 0 || s.q_a() > s.q()) {
                throw new IllegalStateException("allocate output has S q_a outside [0,q]");
            }
            allowedSid.add(s.id());
        }

        for (TS ts : emittedTs) {
            if (!pid.equals(ts.pid())) {
                throw new IllegalStateException("allocate output has TS with different pid");
            }
            if (!allowedTid.contains(ts.tid())) {
                throw new IllegalStateException("allocate output references unknown TS.tid " + ts.tid());
            }
            if (!allowedSid.contains(ts.sid())) {
                throw new IllegalStateException("allocate output references unknown TS.sid " + ts.sid());
            }
            long sourceQ = tQuantityById.get(ts.tid());
            if (ts.q() != sourceQ) {
                throw new IllegalStateException("allocate output has TS.q that does not match source T.q");
            }
            if (Long.signum(ts.q_a()) != Long.signum(ts.q())) {
                throw new IllegalStateException("allocate output has TS q_a sign different from TS.q sign");
            }
            if (Math.abs(ts.q_a()) > Math.abs(ts.q())) {
                throw new IllegalStateException("allocate output has TS abs(q_a) > abs(q)");
            }
            long allocatedForTid = allocatedByTid.getOrDefault(ts.tid(), 0L) + ts.q_a();
            allocatedByTid.put(ts.tid(), allocatedForTid);
            if (Math.abs(allocatedForTid) > Math.abs(sourceQ)) {
                throw new IllegalStateException("allocate output over-allocates T id " + ts.tid());
            }
        }
    }

    private void persistUpdatedS(String pid, List<S> updatedCandidates, Record<String, MessageEnvelope> record, OrdinalRef ordinal) {
        List<S> open = new ArrayList<>();
        for (S s : updatedCandidates) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("Updated S pid mismatch");
            }
            if (isOpen(s)) {
                open.add(s);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, s, null, record, ordinal.next());
            } else {
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_S, null, null, null, record, ordinal.next());
            }
        }
        sStore.put(pid, new SBucket(open));
    }

    private void persistUpdatedT(String pid, List<T> updatedCandidates, Record<String, MessageEnvelope> record, OrdinalRef ordinal) {
        List<T> open = new ArrayList<>();
        for (T t : updatedCandidates) {
            if (!pid.equals(t.pid())) {
                throw new IllegalStateException("Updated T pid mismatch");
            }
            if (isOpen(t)) {
                open.add(t);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, t, null, null, record, ordinal.next());
            } else {
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, null, null, null, record, ordinal.next());
            }
        }
        tStore.put(pid, new TBucket(open));
    }

    private List<S> loadS(String pid) {
        SBucket bucket = sStore.get(pid);
        return bucket == null ? List.of() : bucket.items();
    }

    private List<T> loadT(String pid) {
        TBucket bucket = tStore.get(pid);
        return bucket == null ? List.of() : bucket.items();
    }

    private void upsertT(String pid, T value) {
        List<T> next = new ArrayList<>();
        boolean replaced = false;
        for (T t : loadT(pid)) {
            if (t.id().equals(value.id())) {
                next.add(value);
                replaced = true;
            } else {
                next.add(t);
            }
        }
        if (!replaced) {
            next.add(value);
        }
        tStore.put(pid, new TBucket(next));
    }

    private void removeT(String pid, String id) {
        List<T> next = new ArrayList<>();
        for (T t : loadT(pid)) {
            if (!t.id().equals(id)) {
                next.add(t);
            }
        }
        tStore.put(pid, new TBucket(next));
    }

    private void upsertS(String pid, S value) {
        List<S> next = new ArrayList<>();
        boolean replaced = false;
        for (S s : loadS(pid)) {
            if (s.id().equals(value.id())) {
                next.add(value);
                replaced = true;
            } else {
                next.add(s);
            }
        }
        if (!replaced) {
            next.add(value);
        }
        sStore.put(pid, new SBucket(next));
    }

    private void removeS(String pid, String id) {
        List<S> next = new ArrayList<>();
        for (S s : loadS(pid)) {
            if (!s.id().equals(id)) {
                next.add(s);
            }
        }
        sStore.put(pid, new SBucket(next));
    }

    private void emitProcessed(String pid, MessageEnvelope value, long timestamp) {
        context().forward(new Record<>(pid, value, timestamp), TopologyFactory.PROCESSED_SINK);
    }

    private void emitDbSync(String pid,
                            DbSyncMutationType mutationType,
                            T t,
                            S s,
                            TS ts,
                            Record<String, MessageEnvelope> source,
                            int ordinal) {
        RecordMetadata metadata = context().recordMetadata()
                .orElseThrow(() -> new IllegalStateException("Record metadata is required for db-sync events"));

        DbSyncEnvelope event = new DbSyncEnvelope(
                mutationType,
                pid,
                t,
                s,
                ts,
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                eventTimestamp(source),
                ordinal,
                buildEventId(metadata, mutationType, ordinal)
        );

        context().forward(new Record<>(pid, event, source.timestamp()), TopologyFactory.DB_SYNC_SINK);
    }

    private static String buildEventId(RecordMetadata metadata, DbSyncMutationType mutationType, int ordinal) {
        return metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset() + "-" + ordinal + "-" + mutationType;
    }

    private String buildTsIdPrefix(Record<String, MessageEnvelope> record, String kind, String id) {
        RecordMetadata metadata = context().recordMetadata()
                .orElseThrow(() -> new IllegalStateException("Record metadata is required for TS IDs"));
        return "ts-" + kind + "-" + id + "-" + metadata.partition() + "-" + metadata.offset();
    }

    private long eventTimestamp(Record<String, MessageEnvelope> record) {
        return record.timestamp() >= 0 ? record.timestamp() : context().currentSystemTimeMs();
    }

    private static String buildDedupeKey(T t) {
        return t.pid() + "|" + t.ref() + "|" + t.cancel();
    }

    private void evictExpiredDedupeKeys(long streamTime) {
        long cutoff = streamTime - DEDUPE_WINDOW_MILLIS;
        List<String> toDelete = new ArrayList<>();

        try (KeyValueIterator<String, Long> all = tDedupeStore.all()) {
            while (all.hasNext()) {
                var next = all.next();
                if (next.value != null && next.value < cutoff) {
                    toDelete.add(next.key);
                }
            }
        }

        for (String key : toDelete) {
            tDedupeStore.delete(key);
        }

        if (!toDelete.isEmpty()) {
            log.info("Evicted {} expired dedupe keys older than {}", toDelete.size(), cutoff);
        }
    }

    private static void requireSamePid(List<String> pids, String expectedPid, String entityName) {
        for (String pid : pids) {
            if (!expectedPid.equals(pid)) {
                throw new IllegalArgumentException(entityName + " pid mismatch");
            }
        }
    }

    private static boolean isOpen(T t) {
        return t.q_a() < t.q();
    }

    private static boolean isOpen(S s) {
        return s.q_a() < s.q();
    }

    private static long remaining(long q, long qA) {
        long remaining = q - qA;
        return Math.max(remaining, 0);
    }

    private record AllocationResult(
            T updatedIncomingT,
            List<S> updatedS,
            S updatedIncomingS,
            List<T> updatedT,
            List<TS> emittedTs
    ) {
        private AllocationResult {
            updatedS = List.copyOf(updatedS);
            updatedT = List.copyOf(updatedT);
            emittedTs = List.copyOf(emittedTs);
        }

        private AllocationResult(T updatedIncomingT, List<S> updatedS, List<TS> emittedTs) {
            this(updatedIncomingT, updatedS, null, List.of(), emittedTs);
        }
    }

    private static final class OrdinalRef {
        private int current;

        private OrdinalRef(int start) {
            this.current = start;
        }

        private int next() {
            return current++;
        }
    }
}
