package com.example.stateful.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.MessageEnvelope;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyFactoryTest {

    @Test
    void tIsStoredAndEmitsTsWhileSIsOnlyStored() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SerdeFactory serdeFactory = new SerdeFactory(mapper);
        ProcessorSettings settings = new ProcessorSettings(
                "dummy:9092",
                "topology-test-app",
                "input-events",
                "processed-events",
                Files.createTempDirectory("streams-test-state"),
                0,
                3,
                100
        );

        Topology topology = TopologyFactory.create(settings, serdeFactory);
        Properties properties = settings.toStreamsProperties();
        properties.put("bootstrap.servers", "dummy:9092");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, properties, Instant.parse("2026-01-01T00:00:00Z"))) {
            TestInputTopic<String, MessageEnvelope> inputTopic = driver.createInputTopic(
                    settings.inputTopic(),
                    Serdes.String().serializer(),
                    serdeFactory.envelopeSerde().serializer()
            );
            TestOutputTopic<String, MessageEnvelope> outputTopic = driver.createOutputTopic(
                    settings.outputTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.envelopeSerde().deserializer()
            );

            inputTopic.pipeInput(null, MessageEnvelope.forT(new T("t-1", "IBM", 100)));
            inputTopic.pipeInput(null, MessageEnvelope.forS(new S("s-1", "IBM", 60)));

            assertThat(outputTopic.readKeyValue()).extracting(kv -> kv.key, kv -> kv.value.ts().id(), kv -> kv.value.ts().pid(), kv -> kv.value.ts().q())
                    .containsExactly("IBM", "ts-t-1", "IBM", 100L);
            assertThat(outputTopic.isEmpty()).isTrue();

            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
            KeyValueStore<String, SBucket> sStore = driver.getKeyValueStore(StateStoresConfig.UNPROCESSED_S_STORE);

            assertThat(tStore.get("IBM").items()).hasSize(1);
            assertThat(tStore.get("IBM").items().get(0).id()).isEqualTo("t-1");
            assertThat(sStore.get("IBM").items()).hasSize(1);
            assertThat(sStore.get("IBM").items().get(0).id()).isEqualTo("s-1");
        }
    }
}
