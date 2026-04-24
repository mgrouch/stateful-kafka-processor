package com.example.stateful.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public final class ProcessedEventsForwardProcessor extends ContextualProcessor<String, ProcessorForward, String, com.example.stateful.messaging.MessageEnvelope> {

    @Override
    public void process(Record<String, ProcessorForward> record) {
        if (record.value() == null || record.value().processedEvent() == null) {
            return;
        }
        context().forward(record.withValue(record.value().processedEvent()));
    }
}
