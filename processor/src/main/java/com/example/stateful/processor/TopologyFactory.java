package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public final class TopologyFactory {

    static final String SOURCE = "input-events-source";
    static final String PROCESSOR = "stateful-envelope-processor";
    static final String PROCESSED_SINK = "processed-events-sink";
    static final String DB_SYNC_SINK = "db-sync-events-sink";

    private TopologyFactory() {
    }

    public static Topology create(ProcessorSettings settings, SerdeFactory serdeFactory) {
        Topology topology = new Topology();

        Serde<String> stringSerde = serdeFactory.stringSerde();
        Serde<MessageEnvelope> envelopeSerde = serdeFactory.envelopeSerde();
        Serde<DbSyncEnvelope> dbSyncSerde = serdeFactory.dbSyncEnvelopeSerde();

        topology.addSource(
                SOURCE,
                stringSerde.deserializer(),
                envelopeSerde.deserializer(),
                settings.inputTopic()
        );

        ProcessorSupplier<String, MessageEnvelope, String, Object> supplier = StatefulEnvelopeProcessor::new;
        topology.addProcessor(PROCESSOR, supplier, SOURCE);

        KeyValueBytesStoreSupplier tStoreSupplier = Stores.persistentKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
        KeyValueBytesStoreSupplier sStoreSupplier = Stores.persistentKeyValueStore(StateStoresConfig.UNPROCESSED_S_STORE);
        KeyValueBytesStoreSupplier dedupeStoreSupplier = Stores.persistentKeyValueStore(StateStoresConfig.T_DEDUPE_STORE);

        topology.addStateStore(
                Stores.keyValueStoreBuilder(tStoreSupplier, stringSerde, serdeFactory.tBucketSerde()),
                PROCESSOR
        );
        topology.addStateStore(
                Stores.keyValueStoreBuilder(sStoreSupplier, stringSerde, serdeFactory.sBucketSerde()),
                PROCESSOR
        );
        topology.addStateStore(
                Stores.keyValueStoreBuilder(dedupeStoreSupplier, stringSerde, Serdes.Long()),
                PROCESSOR
        );

        topology.addSink(
                PROCESSED_SINK,
                settings.outputTopic(),
                stringSerde.serializer(),
                envelopeSerde.serializer(),
                PROCESSOR
        );

        topology.addSink(
                DB_SYNC_SINK,
                settings.dbSyncTopic(),
                stringSerde.serializer(),
                dbSyncSerde.serializer(),
                PROCESSOR
        );

        return topology;
    }

    public static String describe(Topology topology) {
        TopologyDescription description = topology.describe();
        return description.toString();
    }
}
