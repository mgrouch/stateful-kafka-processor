package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;

public record ProcessorForward(MessageEnvelope processedEvent, DbSyncEnvelope dbSyncEvent) {

    public static ProcessorForward processed(MessageEnvelope value) {
        return new ProcessorForward(value, null);
    }

    public static ProcessorForward dbSync(DbSyncEnvelope value) {
        return new ProcessorForward(null, value);
    }
}
