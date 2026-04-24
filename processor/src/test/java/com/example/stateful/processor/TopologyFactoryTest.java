package com.example.stateful.processor;

import com.example.stateful.domain.T;
import com.example.stateful.messaging.MessageEnvelope;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyFactoryTest {

    @Test
    void tDeduplicationUsesPidRefCancelWithTwoWeekWindow() throws Exception {
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
                100
        );

        Topology topology = TopologyFactory.create(settings, serdeFactory);
        Properties properties = settings.toStreamsProperties();
        properties.put("bootstrap.servers", "dummy:9092");

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long withinWindow = t0.plus(Duration.ofDays(13)).toEpochMilli();
        long outsideWindow = t0.plus(Duration.ofDays(15)).toEpochMilli();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, properties, t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = driver.createInputTopic(
                    settings.inputTopic(),
                    Serdes.String().serializer(),
                    serdeFactory.envelopeSerde().serializer(),
                    t0,
                    Duration.ofMillis(1)
            );
            TestOutputTopic<String, MessageEnvelope> outputTopic = driver.createOutputTopic(
                    settings.outputTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.envelopeSerde().deserializer()
            );

            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L)), t0.toEpochMilli());
            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-2", "IBM", "R-1", false, 101L)), withinWindow);
            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-3", "IBM", "R-1", false, 102L)), outsideWindow);

            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-4", "IBM", "R-2", false, 103L)), withinWindow);
            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-5", "IBM", "R-1", true, 104L)), withinWindow);
            inputTopic.pipeInput("MSFT", MessageEnvelope.forT(new T("t-6", "MSFT", "R-1", false, 105L)), withinWindow);

            assertThat(outputTopic.readKeyValuesToList())
                    .extracting(kv -> kv.key, kv -> kv.value.ts().id())
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-1"),
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-3"),
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-4"),
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-5"),
                            org.assertj.core.groups.Tuple.tuple("MSFT", "ts-t-6")
                    );

            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
            KeyValueStore<String, Long> dedupeStore = driver.getKeyValueStore(StateStoresConfig.T_DEDUPE_STORE);

            assertThat(tStore.get("IBM").items()).extracting(T::id).containsExactly("t-1", "t-3", "t-4", "t-5");
            assertThat(tStore.get("MSFT").items()).extracting(T::id).containsExactly("t-6");

            assertThat(dedupeStore.get("IBM|R-1|false")).isEqualTo(outsideWindow);
            assertThat(dedupeStore.get("IBM|R-2|false")).isEqualTo(withinWindow);
            assertThat(dedupeStore.get("IBM|R-1|true")).isEqualTo(withinWindow);
            assertThat(dedupeStore.get("MSFT|R-1|false")).isEqualTo(withinWindow);
        }
    }

    @Test
    void dedupeStoreIsCleanedUpAfterWindowViaStreamTimePunctuation() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SerdeFactory serdeFactory = new SerdeFactory(mapper);
        ProcessorSettings settings = new ProcessorSettings(
                "dummy:9092",
                "topology-test-app-cleanup",
                "input-events",
                "processed-events",
                Files.createTempDirectory("streams-test-state-cleanup"),
                100
        );

        Topology topology = TopologyFactory.create(settings, serdeFactory);
        Properties properties = settings.toStreamsProperties();
        properties.put("bootstrap.servers", "dummy:9092");

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long oldTs = t0.toEpochMilli();
        long triggerCleanupTs = t0.plus(Duration.ofDays(16)).toEpochMilli();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, properties, t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = driver.createInputTopic(
                    settings.inputTopic(),
                    Serdes.String().serializer(),
                    serdeFactory.envelopeSerde().serializer()
            );

            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L)), oldTs);
            inputTopic.pipeInput("GOOG", MessageEnvelope.forT(new T("t-2", "GOOG", "R-9", false, 110L)), triggerCleanupTs);

            KeyValueStore<String, Long> dedupeStore = driver.getKeyValueStore(StateStoresConfig.T_DEDUPE_STORE);
            assertThat(dedupeStore.get("IBM|R-1|false")).isNull();
            assertThat(dedupeStore.get("GOOG|R-9|false")).isEqualTo(triggerCleanupTs);
        }
    }
}
