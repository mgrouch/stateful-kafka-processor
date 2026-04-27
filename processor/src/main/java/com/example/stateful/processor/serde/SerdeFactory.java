package com.example.stateful.processor.serde;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.domain.ReconReport;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.messaging.MessageEnvelopeAvroCodec;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.TSBucket;
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
        return Serdes.serdeFrom(
                (topic, data) -> MessageEnvelopeAvroCodec.serialize(data),
                (topic, data) -> MessageEnvelopeAvroCodec.deserialize(data)
        );
    }

    public Serde<DbSyncEnvelope> dbSyncEnvelopeSerde() {
        return new JsonSerde<>(objectMapper, DbSyncEnvelope.class);
    }

    public Serde<TSBucket> tsBucketSerde() {
        return new JsonSerde<>(objectMapper, TSBucket.class);
    }

    public Serde<SBucket> sBucketSerde() {
        return new JsonSerde<>(objectMapper, SBucket.class);
    }

    public Serde<ReconReport> reconReportSerde() {
        return new JsonSerde<>(objectMapper, ReconReport.class);
    }

}

