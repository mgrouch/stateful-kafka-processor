package com.example.stateful.processor.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.logic.AllocationResult;
import com.example.stateful.processor.logic.AllocationStrategy;
import com.example.stateful.processor.logic.TransitionsModel;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class StatefulEnvelopeProcessor extends ContextualProcessor<String, MessageEnvelope, String, Object> {

    private static final Logger log = LoggerFactory.getLogger(StatefulEnvelopeProcessor.class);

    static final long DEDUPE_WINDOW_MILLIS = Duration.ofDays(14).toMillis();
    private static final Duration DEDUPE_CLEANUP_INTERVAL = Duration.ofHours(1);
    private final TransitionsModel transitionsLogic;

    private KeyValueStore<String, TBucket> tStore;
    private KeyValueStore<String, SBucket> sStore;
    private KeyValueStore<String, Long> tDedupeStore;

    public StatefulEnvelopeProcessor(AllocationStrategy allocationStrategy) {
        this.transitionsLogic = new TransitionsModel(allocationStrategy);
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

        if (incomingT.cancel() && tryProcessCancellation(record, pid, incomingT, ordinal)) {
            return;
        }

        List<S> candidates = loadS(pid);
        AllocationResult allocation = transitionsLogic.allocateForIncomingT(incomingT, candidates, buildTsIdPrefix(record, "t", incomingT.id()));

        for (TS ts : allocation.emittedTs()) {
            emitProcessed(pid, MessageEnvelope.forTS(ts), record.timestamp());
            emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, ts, record, ordinal++);
        }

        persistUpdatedS(pid, allocation.updatedS(), record, new OrdinalRef(ordinal));
        ordinal = ordinal + allocation.updatedS().size();

        if (transitionsLogic.isOpen(allocation.updatedIncomingT())) {
            upsertT(pid, allocation.updatedIncomingT());
            emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, allocation.updatedIncomingT(), null, null, record, ordinal);
        } else {
            removeT(pid, allocation.updatedIncomingT().id());
            emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, allocation.updatedIncomingT(), null, null, record, ordinal);
        }

        log.info("Processed T id={} pid={} q={} q_a_total={}", allocation.updatedIncomingT().id(), pid, allocation.updatedIncomingT().q(), allocation.updatedIncomingT().q_a_total());
    }

    private boolean tryProcessCancellation(Record<String, MessageEnvelope> record, String pid, T incomingT, int ordinal) {
        T openTrade = loadT(pid).stream()
                .filter(t -> t.id().equals(incomingT.id()))
                .findFirst()
                .orElse(null);
        if (openTrade == null || openTrade.q_a_total() != 0L) {
            return false;
        }

        long cancelDelta = openTrade.q() - openTrade.q_a_total();
        T processedTrade = new T(
                openTrade.id(),
                openTrade.pid(),
                openTrade.ref(),
                openTrade.accId(),
                openTrade.tt(),
                openTrade.tDate(),
                openTrade.sDate(),
                openTrade.a_status(),
                true,
                openTrade.q(),
                openTrade.q(),
                cancelDelta,
                openTrade.q_f(),
                openTrade.ledgerTime()
        );
        TS cancelTs = new TS(
                buildTsIdPrefix(record, "cancel", incomingT.id()) + "-1",
                pid,
                processedTrade.id(),
                "cancel-" + processedTrade.id(),
                processedTrade.accId(),
                processedTrade.tDate(),
                processedTrade.sDate(),
                processedTrade.q(),
                cancelDelta,
                processedTrade.q_a_total(),
                processedTrade.tt(),
                true
        );

        emitProcessed(pid, MessageEnvelope.forTS(cancelTs), record.timestamp());
        emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, cancelTs, record, ordinal++);
        removeT(pid, processedTrade.id());
        emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, processedTrade, null, null, record, ordinal);
        log.info("Processed cancellation for T id={} pid={} q={}", processedTrade.id(), pid, processedTrade.q());
        return true;
    }

    private void handleS(Record<String, MessageEnvelope> record, String pid, int ordinal) {
        S incomingS = record.value().s();
        emitDbSync(pid, DbSyncMutationType.ACCEPTED_S, null, incomingS, null, record, ordinal++);

        List<T> candidates = loadT(pid);
        AllocationResult allocation = transitionsLogic.allocateForIncomingS(candidates, incomingS, buildTsIdPrefix(record, "s", incomingS.id()));

        for (TS ts : allocation.emittedTs()) {
            emitProcessed(pid, MessageEnvelope.forTS(ts), record.timestamp());
            emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, ts, record, ordinal++);
        }

        persistUpdatedT(pid, allocation.updatedT(), record, new OrdinalRef(ordinal));
        ordinal = ordinal + allocation.updatedT().size();

        if (transitionsLogic.isOpen(allocation.updatedIncomingS())) {
            upsertS(pid, allocation.updatedIncomingS());
            emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, allocation.updatedIncomingS(), null, record, ordinal);
        } else {
            removeS(pid, allocation.updatedIncomingS().id());
            emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_S, null, allocation.updatedIncomingS(), null, record, ordinal);
        }

        log.info("Processed S id={} pid={} q={} q_a={}", allocation.updatedIncomingS().id(), pid, allocation.updatedIncomingS().q(), allocation.updatedIncomingS().q_a());
    }

    private void handleTs(Record<String, MessageEnvelope> record, String pid, int ordinalStart) {
        log.info("Forwarding TS id={} pid={}", record.value().ts().id(), pid);
        emitProcessed(pid, record.value(), record.timestamp());
        emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, record.value().ts(), record, ordinalStart);
    }

    private void persistUpdatedS(String pid, List<S> updatedCandidates, Record<String, MessageEnvelope> record, OrdinalRef ordinal) {
        Map<String, S> openById = new LinkedHashMap<>();
        Set<String> closedIds = new HashSet<>();
        for (S s : updatedCandidates) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("Updated S pid mismatch");
            }
            if (transitionsLogic.isOpen(s)) {
                openById.put(s.id(), s);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, s, null, record, ordinal.next());
            } else {
                closedIds.add(s.id());
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_S, null, s, null, record, ordinal.next());
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
            if (transitionsLogic.isOpen(t)) {
                openById.put(t.id(), t);
                emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, t, null, null, record, ordinal.next());
            } else {
                closedIds.add(t.id());
                emitDbSync(pid, DbSyncMutationType.DELETE_UNPROCESSED_T, t, null, null, record, ordinal.next());
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
