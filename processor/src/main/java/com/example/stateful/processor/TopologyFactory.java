package com.example.stateful.processor;

import com.example.stateful.messaging.MessageEnvelope;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public final class TopologyFactory {

    private static final String SOURCE = "input-events-source";
    private static final String PROCESSOR = "stateful-envelope-processor";
    private static final String SINK = "processed-events-sink";

    private TopologyFactory() {
    }

    public static Topology create(ProcessorSettings settings, SerdeFactory serdeFactory) {
        Topology topology = new Topology();

        Serde<String> stringSerde = serdeFactory.stringSerde();
        Serde<MessageEnvelope> envelopeSerde = serdeFactory.envelopeSerde();

        topology.addSource(
                SOURCE,
                stringSerde.deserializer(),
                envelopeSerde.deserializer(),
                settings.inputTopic()
        );

        ProcessorSupplier<String, MessageEnvelope, String, MessageEnvelope> supplier = () -> new StatefulEnvelopeProcessor(settings);
        topology.addProcessor(PROCESSOR, supplier, SOURCE);

        KeyValueBytesStoreSupplier tStoreSupplier = Stores.persistentKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
        KeyValueBytesStoreSupplier sStoreSupplier = Stores.persistentKeyValueStore(StateStoresConfig.UNPROCESSED_S_STORE);

        topology.addStateStore(
                Stores.keyValueStoreBuilder(tStoreSupplier, stringSerde, serdeFactory.tBucketSerde()),
                PROCESSOR
        );
        topology.addStateStore(
                Stores.keyValueStoreBuilder(sStoreSupplier, stringSerde, serdeFactory.sBucketSerde()),
                PROCESSOR
        );

        topology.addSink(
                SINK,
                settings.outputTopic(),
                stringSerde.serializer(),
                envelopeSerde.serializer(),
                PROCESSOR
        );

        return topology;
    }

    public static String describe(Topology topology) {
        TopologyDescription description = topology.describe();
        return description.toString();
    }
}
