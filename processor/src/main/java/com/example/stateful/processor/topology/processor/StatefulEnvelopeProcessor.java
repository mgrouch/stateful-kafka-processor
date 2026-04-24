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
import java.util.List;

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

    private void handleT(Record<String, MessageEnvelope> record, String pid, int ordinalStart) {
        T t = record.value().t();
        long timestamp = eventTimestamp(record);
        String dedupeKey = buildDedupeKey(t);
        Long seenAt = tDedupeStore.get(dedupeKey);

        if (seenAt != null && timestamp - seenAt <= DEDUPE_WINDOW_MILLIS) {
            log.info("Skipping duplicate T id={} pid={} dedupeKey={} seenAt={} currentTs={}", t.id(), pid, dedupeKey, seenAt, timestamp);
            return;
        }

        tDedupeStore.put(dedupeKey, timestamp);

        TBucket current = tStore.get(pid);
        TBucket updated = (current == null ? TBucket.empty() : current).append(t);
        tStore.put(pid, updated);

        TS ts = new TS("ts-" + t.id(), pid, t.q());
        MessageEnvelope output = MessageEnvelope.forTS(ts);

        emitProcessed(pid, output, record.timestamp());
        emitDbSync(pid, DbSyncMutationType.ACCEPTED_T, t, null, null, record, ordinalStart);
        emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_T, t, null, null, record, ordinalStart + 1);
        emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, ts, record, ordinalStart + 2);

        log.info("Stored T id={} pid={} ref={} cancel={}", t.id(), pid, t.ref(), t.cancel());
    }

    private void handleS(Record<String, MessageEnvelope> record, String pid, int ordinalStart) {
        S s = record.value().s();

        SBucket current = sStore.get(pid);
        SBucket updated = (current == null ? SBucket.empty() : current).append(s);
        sStore.put(pid, updated);

        emitDbSync(pid, DbSyncMutationType.ACCEPTED_S, null, s, null, record, ordinalStart);
        emitDbSync(pid, DbSyncMutationType.UPSERT_UNPROCESSED_S, null, s, null, record, ordinalStart + 1);

        log.info("Stored S id={} pid={} at {}", s.id(), pid, Instant.ofEpochMilli(record.timestamp()));
    }

    private void handleTs(Record<String, MessageEnvelope> record, String pid, int ordinalStart) {
        log.info("Forwarding TS id={} pid={}", record.value().ts().id(), pid);
        emitProcessed(pid, record.value(), record.timestamp());
        emitDbSync(pid, DbSyncMutationType.GENERATED_TS, null, null, record.value().ts(), record, ordinalStart);
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
}
