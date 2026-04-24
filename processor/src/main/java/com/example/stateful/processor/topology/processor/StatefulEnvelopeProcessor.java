package com.example.stateful.processor.topology.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.domain.AllocationStatus;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Collections;

public final class StatefulEnvelopeProcessor extends ContextualProcessor<String, MessageEnvelope, String, Object> {

    private static final Logger log = LoggerFactory.getLogger(StatefulEnvelopeProcessor.class);

    static final long DEDUPE_WINDOW_MILLIS = Duration.ofDays(14).toMillis();
    private static final Duration DEDUPE_CLEANUP_INTERVAL = Duration.ofHours(1);
    private static final String LOTTERY_INCOMING_S = "INCOMING_S";
    private static final String LOTTERY_FAIL_BUCKET = "FAIL";
    private static final String LOTTERY_NORMAL_BUCKET = "NORMAL";

    private final long allocationLotterySeed;

    private KeyValueStore<String, TBucket> tStore;
    private KeyValueStore<String, SBucket> sStore;
    private KeyValueStore<String, Long> tDedupeStore;

    public StatefulEnvelopeProcessor() {
        this(1357911L);
    }

    public StatefulEnvelopeProcessor(long allocationLotterySeed) {
        this.allocationLotterySeed = allocationLotterySeed;
    }

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
        long incomingTOpen = remainingT(incomingT);
        List<S> signCompatibleCandidates = candidates.stream()
                .filter(candidate -> areSignCompatible(incomingTOpen, remainingS(candidate)))
                .toList();
        List<S> untouchedCandidates = candidates.stream()
                .filter(candidate -> !areSignCompatible(incomingTOpen, remainingS(candidate)))
                .toList();
        List<S> orderedCandidates = orderSCandidates(signCompatibleCandidates);
        ensureRolloverPriority(orderedCandidates);

        List<S> updatedS = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        T updatedT = incomingT;
        int tsIndex = 0;

        for (S candidate : orderedCandidates) {
            long tRemaining = remainingT(updatedT);
            long sRemaining = remainingS(candidate);
            long allocated = signedAllocation(tRemaining, sRemaining);

            if (allocated != 0) {
                updatedT = new T(updatedT.id(), updatedT.pid(), updatedT.ref(), updatedT.cancel(), updatedT.q(), updatedT.q_a() + allocated, updatedT.a_status());
                S nextS = new S(candidate.id(), candidate.pid(), candidate.q(), candidate.q_carry(), candidate.q_a() + allocated, candidate.rollover());
                updatedS.add(nextS);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedT.pid(), updatedT.id(), nextS.id(), updatedT.q(), allocated));
            } else {
                updatedS.add(candidate);
            }
        }
        updatedS.addAll(untouchedCandidates);

        validateAllocationOutput(updatedT.pid(), List.of(updatedT), updatedS, emitted);
        return new AllocationResult(updatedT, updatedS, emitted);
    }

    private AllocationResult allocateForIncomingS(List<T> candidates, S incomingS, String idPrefix) {
        long incomingSOpen = remainingS(incomingS);
        List<T> signCompatibleCandidates = candidates.stream()
                .filter(candidate -> areSignCompatible(incomingSOpen, remainingT(candidate)))
                .toList();
        List<T> untouchedCandidates = candidates.stream()
                .filter(candidate -> !areSignCompatible(incomingSOpen, remainingT(candidate)))
                .toList();
        List<T> orderedCandidates = orderTCandidates(signCompatibleCandidates, incomingS.pid(), incomingS.id());
        requireSamePid(orderedCandidates.stream().map(T::pid).toList(), incomingS.pid(), "T candidate");
        ensureFailPriority(orderedCandidates);

        List<T> updatedT = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        S updatedS = incomingS;
        int tsIndex = 0;

        for (T candidate : orderedCandidates) {
            long sRemaining = remainingS(updatedS);
            long tRemaining = remainingT(candidate);
            long allocated = signedAllocation(sRemaining, tRemaining);

            if (allocated != 0) {
                T nextT = new T(candidate.id(), candidate.pid(), candidate.ref(), candidate.cancel(), candidate.q(), candidate.q_a() + allocated, candidate.a_status());
                updatedS = new S(updatedS.id(), updatedS.pid(), updatedS.q(), updatedS.q_carry(), updatedS.q_a() + allocated, updatedS.rollover());
                updatedT.add(nextT);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedS.pid(), nextT.id(), updatedS.id(), nextT.q(), allocated));
            } else {
                updatedT.add(candidate);
            }
        }
        updatedT.addAll(untouchedCandidates);

        validateAllocationOutput(updatedS.pid(), updatedT, List.of(updatedS), emitted);
        return new AllocationResult(null, List.of(), updatedS, updatedT, emitted);
    }

    private List<S> orderSCandidates(List<S> candidates) {
        return candidates.stream()
                .sorted(Comparator
                        .comparing((S s) -> !s.rollover())
                        .thenComparing(S::id))
                .toList();
    }

    private List<T> orderTCandidates(List<T> candidates, String pid, String incomingId) {
        List<T> canonical = candidates.stream().sorted(Comparator.comparing(T::id)).toList();
        List<T> fail = canonical.stream().filter(t -> t.a_status() == AllocationStatus.FAIL).toList();
        List<T> normal = canonical.stream().filter(t -> t.a_status() != AllocationStatus.FAIL).toList();

        List<T> ordered = new ArrayList<>(candidates.size());
        ordered.addAll(shuffleDeterministically(fail, pid, incomingId, LOTTERY_INCOMING_S, LOTTERY_FAIL_BUCKET));
        ordered.addAll(shuffleDeterministically(normal, pid, incomingId, LOTTERY_INCOMING_S, LOTTERY_NORMAL_BUCKET));
        return ordered;
    }

    private List<T> shuffleDeterministically(List<T> bucket, String pid, String incomingId, String direction, String bucketName) {
        List<T> shuffled = new ArrayList<>(bucket);
        long seed = deriveAllocationSeed(pid, incomingId, direction, bucketName);
        Collections.shuffle(shuffled, new Random(seed));
        return shuffled;
    }

    private long deriveAllocationSeed(String pid, String incomingId, String direction, String bucketName) {
        return Objects.hash(allocationLotterySeed, pid, incomingId, direction, bucketName);
    }

    private static void ensureFailPriority(List<T> orderedCandidates) {
        boolean seenNormal = false;
        for (T candidate : orderedCandidates) {
            if (candidate.a_status() == AllocationStatus.FAIL) {
                if (seenNormal) {
                    throw new IllegalStateException("FAIL T must be ordered before NORMAL T");
                }
            } else {
                seenNormal = true;
            }
        }
    }

    private static void ensureRolloverPriority(List<S> orderedCandidates) {
        boolean seenNonRollover = false;
        for (S candidate : orderedCandidates) {
            if (candidate.rollover()) {
                if (seenNonRollover) {
                    throw new IllegalStateException("rollover S must be ordered before non-rollover S");
                }
            } else {
                seenNonRollover = true;
            }
        }
    }

    private void validateAllocationOutput(String pid, List<T> allowedT, List<S> allowedS, List<TS> emittedTs) {
        Set<String> allowedTid = new HashSet<>();
        Set<String> allowedSid = new HashSet<>();
        Map<String, Long> tQuantityById = new HashMap<>();
        Map<String, Long> allocatedByTid = new HashMap<>();
        for (T t : allowedT) {
            if (!pid.equals(t.pid())) {
                throw new IllegalStateException("allocate output has T with different pid");
            }
            if (!isAllocatedWithinTotal(t.q(), t.q_a())) {
                throw new IllegalStateException("allocate output has T q_a outside signed bounds of q");
            }
            allowedTid.add(t.id());
            tQuantityById.put(t.id(), t.q());
        }
        for (S s : allowedS) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("allocate output has S with different pid");
            }
            if (!isAllocatedWithinTotal(s.q() + s.q_carry(), s.q_a())) {
                throw new IllegalStateException("allocate output has S q_a outside signed bounds of q + q_carry");
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
            if (ts.q_a() == 0) {
                throw new IllegalStateException("allocate output has TS.q_a == 0");
            }
            long allocatedForTid = allocatedByTid.getOrDefault(ts.tid(), 0L) + ts.q_a();
            allocatedByTid.put(ts.tid(), allocatedForTid);
            long sourceQ = tQuantityById.get(ts.tid());
            if (!isAllocatedWithinTotal(sourceQ, allocatedForTid)) {
                throw new IllegalStateException("allocate output over-allocates T id " + ts.tid());
            }
        }
    }

    private void persistUpdatedS(String pid, List<S> updatedCandidates, Record<String, MessageEnvelope> record, OrdinalRef ordinal) {
        Map<String, S> openById = new LinkedHashMap<>();
        Set<String> closedIds = new HashSet<>();
        for (S s : updatedCandidates) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("Updated S pid mismatch");
            }
            if (isOpen(s)) {
                openById.put(s.id(), s);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, s, null, record, ordinal.next());
            } else {
                closedIds.add(s.id());
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_S, null, null, null, record, ordinal.next());
            }
        }
        closedIds.forEach(openById::remove);
        sStore.put(pid, new SBucket(new ArrayList<>(openById.values())));
    }

    private void persistUpdatedT(String pid, List<T> updatedCandidates, Record<String, MessageEnvelope> record, OrdinalRef ordinal) {
        Map<String, T> openById = new LinkedHashMap<>();
        Set<String> closedIds = new HashSet<>();
        for (T t : updatedCandidates) {
            if (!pid.equals(t.pid())) {
                throw new IllegalStateException("Updated T pid mismatch");
            }
            if (isOpen(t)) {
                openById.put(t.id(), t);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, t, null, null, record, ordinal.next());
            } else {
                closedIds.add(t.id());
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, null, null, null, record, ordinal.next());
            }
        }
        closedIds.forEach(openById::remove);
        tStore.put(pid, new TBucket(new ArrayList<>(openById.values())));
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
        return remainingT(t) != 0;
    }

    private static boolean isOpen(S s) {
        return remainingS(s) != 0;
    }

    private static long remainingT(T t) {
        return signedRemaining(t.q(), t.q_a());
    }

    private static long remainingS(S s) {
        SignedSupplyUsage usage = supplyUsage(s);
        return usage.remainingCarry() + usage.remainingRegular();
    }

    private static long signedRemaining(long total, long allocated) {
        return total - allocated;
    }

    private static boolean areSignCompatible(long lhs, long rhs) {
        return lhs != 0 && rhs != 0 && Long.signum(lhs) == Long.signum(rhs);
    }

    private static long signedAllocation(long targetOpen, long sourceOpen) {
        if (!areSignCompatible(targetOpen, sourceOpen)) {
            return 0L;
        }
        long magnitude = Math.min(Math.abs(targetOpen), Math.abs(sourceOpen));
        return Long.signum(targetOpen) * magnitude;
    }

    private static boolean isAllocatedWithinTotal(long total, long allocated) {
        if (allocated == 0L) {
            return true;
        }
        if (total == 0L || Long.signum(total) != Long.signum(allocated)) {
            return false;
        }
        return Math.abs(allocated) <= Math.abs(total);
    }

    /**
     * S allocation always consumes carry bucket before regular bucket.
     * This helper is sign-safe for both positive and negative supplies.
     */
    private static SignedSupplyUsage supplyUsage(S s) {
        long sign = Long.signum(s.q() + s.q_carry());
        long carryMagnitude = Math.abs(s.q_carry());
        long regularMagnitude = Math.abs(s.q());
        long usedMagnitude = Math.abs(s.q_a());

        long carryUsedMagnitude = Math.min(usedMagnitude, carryMagnitude);
        long regularUsedMagnitude = Math.min(Math.max(usedMagnitude - carryMagnitude, 0L), regularMagnitude);

        long remainingCarry = sign * (carryMagnitude - carryUsedMagnitude);
        long remainingRegular = sign * (regularMagnitude - regularUsedMagnitude);
        return new SignedSupplyUsage(remainingCarry, remainingRegular);
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

    private record SignedSupplyUsage(long remainingCarry, long remainingRegular) {
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
