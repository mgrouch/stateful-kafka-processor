package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class SerdeFactory {

    private final ObjectMapper objectMapper;

    public SerdeFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Serde<String> stringSerde() {
        return Serdes.String();
    }

    public Serde<MessageEnvelope> envelopeSerde() {
        return new JsonSerde<>(objectMapper, MessageEnvelope.class);
    }

    public Serde<DbSyncEnvelope> dbSyncEnvelopeSerde() {
        return new JsonSerde<>(objectMapper, DbSyncEnvelope.class);
    }

    public Serde<TBucket> tBucketSerde() {
        return new JsonSerde<>(objectMapper, TBucket.class);
    }

    public Serde<SBucket> sBucketSerde() {
        return new JsonSerde<>(objectMapper, SBucket.class);
    }
}
