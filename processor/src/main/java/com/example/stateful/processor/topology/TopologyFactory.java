package com.example.stateful.processor.topology;

import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.state.StateStores;
import com.example.stateful.processor.logic.AllocationStrategy;
import com.example.stateful.processor.logic.AutoAllocOppositeStrategy;
import com.example.stateful.processor.processor.StatefulEnvelopeProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

public final class TopologyFactory {

    static final String SOURCE = "input-events-source";
    static final String PROCESSOR = "stateful-envelope-processor";
    public static final String PROCESSED_SINK = "processed-events-sink";
    public static final String DB_SYNC_SINK = "db-sync-events-sink";

    private TopologyFactory() {
    }

    public static Topology create(ProcessorSettings settings, SerdeFactory serdeFactory) {
        return create(settings, serdeFactory, new AutoAllocOppositeStrategy(settings.allocationLotterySeed()));
    }

    public static Topology create(ProcessorSettings settings, SerdeFactory serdeFactory, AllocationStrategy allocationStrategy) {
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

        ProcessorSupplier<String, MessageEnvelope, String, Object> supplier =
                () -> new StatefulEnvelopeProcessor(allocationStrategy);
        topology.addProcessor(PROCESSOR, supplier, SOURCE);

        KeyValueBytesStoreSupplier tStoreSupplier = Stores.persistentKeyValueStore(StateStores.UNPROCESSED_T_STORE);
        KeyValueBytesStoreSupplier sStoreSupplier = Stores.persistentKeyValueStore(StateStores.UNPROCESSED_S_STORE);
        KeyValueBytesStoreSupplier dedupeStoreSupplier = Stores.persistentKeyValueStore(StateStores.T_DEDUPE_STORE);

        // Keep changelog topics enabled for all state stores to preserve state recovery guarantees.
        topology.addStateStore(
                loggedStore(Stores.keyValueStoreBuilder(tStoreSupplier, stringSerde, serdeFactory.tBucketSerde())),
                PROCESSOR
        );
        topology.addStateStore(
                loggedStore(Stores.keyValueStoreBuilder(sStoreSupplier, stringSerde, serdeFactory.sBucketSerde())),
                PROCESSOR
        );
        topology.addStateStore(
                loggedStore(Stores.keyValueStoreBuilder(dedupeStoreSupplier, stringSerde, Serdes.Long())),
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

    private static StoreBuilder<?> loggedStore(StoreBuilder<?> storeBuilder) {
        return storeBuilder.withLoggingEnabled(Map.of());
    }
}
