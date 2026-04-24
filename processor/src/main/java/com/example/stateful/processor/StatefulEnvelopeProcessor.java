package com.example.stateful.processor;

import com.example.stateful.domain.TS;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.messaging.MessageKind;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public final class StatefulEnvelopeProcessor extends ContextualProcessor<String, MessageEnvelope, String, MessageEnvelope> {

    private static final Logger log = LoggerFactory.getLogger(StatefulEnvelopeProcessor.class);

    private KeyValueStore<String, TBucket> tStore;
    private KeyValueStore<String, SBucket> sStore;

    @Override
    public void init(ProcessorContext<String, MessageEnvelope> context) {
        super.init(context);
        this.tStore = context.getStateStore(StateStoresConfig.UNPROCESSED_T_STORE);
        this.sStore = context.getStateStore(StateStoresConfig.UNPROCESSED_S_STORE);
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
        switch (record.value().kind()) {
            case T -> handleT(record, pid);
            case S -> handleS(record, pid);
            case TS -> handleTs(record, pid);
            default -> throw new IllegalStateException("Unsupported kind " + record.value().kind());
        }
    }

    private void handleT(Record<String, MessageEnvelope> record, String pid) {
        TBucket current = tStore.get(pid);
        TBucket updated = (current == null ? TBucket.empty() : current).append(record.value().t());
        tStore.put(pid, updated);

        TS ts = new TS("ts-" + record.value().t().id(), pid, record.value().t().q());
        MessageEnvelope output = MessageEnvelope.forTS(ts);

        log.info("Stored T id={} pid={}", record.value().t().id(), pid);

        context().forward(record.withKey(pid).withValue(output));
    }

    private void handleS(Record<String, MessageEnvelope> record, String pid) {
        SBucket current = sStore.get(pid);
        SBucket updated = (current == null ? SBucket.empty() : current).append(record.value().s());
        sStore.put(pid, updated);

        log.info("Stored S id={} pid={} at {}", record.value().s().id(), pid, Instant.ofEpochMilli(record.timestamp()));
    }

    private void handleTs(Record<String, MessageEnvelope> record, String pid) {
        log.info("Forwarding TS id={} pid={}", record.value().ts().id(), pid);
        context().forward(record.withKey(pid));
    }
}
