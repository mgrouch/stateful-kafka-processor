package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public final class DbSyncForwardProcessor extends ContextualProcessor<String, ProcessorForward, String, DbSyncEnvelope> {

    @Override
    public void process(Record<String, ProcessorForward> record) {
        if (record.value() == null || record.value().dbSyncEvent() == null) {
            return;
        }
        context().forward(record.withValue(record.value().dbSyncEvent()));
    }
}
