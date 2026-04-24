package com.example.stateful.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.DbSyncMutationType;
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
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyFactoryTest {

    @Test
    void acceptedTEmitsProcessedAndDbSyncEventsInStableOrderWithPidKey() throws Exception {
        TestHarness harness = new TestHarness();

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> outputTopic = harness.output(driver);
            TestOutputTopic<String, DbSyncEnvelope> dbSyncOutput = harness.dbSyncOutput(driver);

            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L)), t0.toEpochMilli());

            assertThat(outputTopic.readKeyValue())
                    .extracting(kv -> kv.key, kv -> kv.value.ts().id())
                    .containsExactly("IBM", "ts-t-1");

            assertThat(dbSyncOutput.readKeyValuesToList())
                    .extracting(kv -> kv.key, kv -> kv.value.mutationType(), kv -> kv.value.ordinal())
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("IBM", DbSyncMutationType.ACCEPTED_T, 0),
                            org.assertj.core.groups.Tuple.tuple("IBM", DbSyncMutationType.UPSERT_UNPROCESSED_T, 1),
                            org.assertj.core.groups.Tuple.tuple("IBM", DbSyncMutationType.GENERATED_TS, 2)
                    );

            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
            assertThat(tStore.get("IBM").items()).extracting(T::id).containsExactly("t-1");
        }
    }

    @Test
    void acceptedSEmitsDbSyncEventsWithPidKey() throws Exception {
        TestHarness harness = new TestHarness();

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            TestOutputTopic<String, DbSyncEnvelope> dbSyncOutput = harness.dbSyncOutput(driver);

            inputTopic.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 10L)), t0.toEpochMilli());

            List<DbSyncEnvelope> events = dbSyncOutput.readValuesToList();
            assertThat(events).extracting(DbSyncEnvelope::mutationType)
                    .containsExactly(DbSyncMutationType.ACCEPTED_S, DbSyncMutationType.UPSERT_UNPROCESSED_S);
            assertThat(events).extracting(DbSyncEnvelope::pid).containsOnly("IBM");
        }
    }

    @Test
    void tDeduplicationUsesPidRefCancelWithTwoWeekWindow() throws Exception {
        TestHarness harness = new TestHarness();

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long withinWindow = t0.plus(Duration.ofDays(13)).toEpochMilli();
        long outsideWindow = t0.plus(Duration.ofDays(15)).toEpochMilli();

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> outputTopic = harness.output(driver);

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
    void topologyContainsOnlyExpectedStateStores() throws Exception {
        TestHarness harness = new TestHarness();
        Topology topology = TopologyFactory.create(harness.settings, harness.serdeFactory);
        String description = TopologyFactory.describe(topology);
        assertThat(description).contains(StateStoresConfig.UNPROCESSED_T_STORE, StateStoresConfig.UNPROCESSED_S_STORE, StateStoresConfig.T_DEDUPE_STORE);
    }
    private static final class TestHarness {
        private final SerdeFactory serdeFactory;
        private final ProcessorSettings settings;

        private TestHarness() throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            this.serdeFactory = new SerdeFactory(mapper);
            this.settings = new ProcessorSettings(
                    "dummy:9092",
                    "topology-test-app",
                    "input-events",
                    "processed-events",
                    "db-sync-events",
                    Files.createTempDirectory("streams-test-state"),
                    100
            );
        }

        private TopologyTestDriver driver(Instant t0) {
            Topology topology = TopologyFactory.create(settings, serdeFactory);
            Properties properties = settings.toStreamsProperties();
            properties.put("bootstrap.servers", "dummy:9092");
            return new TopologyTestDriver(topology, properties, t0);
        }

        private TestInputTopic<String, MessageEnvelope> input(TopologyTestDriver driver, Instant t0) {
            return driver.createInputTopic(
                    settings.inputTopic(),
                    Serdes.String().serializer(),
                    serdeFactory.envelopeSerde().serializer(),
                    t0,
                    Duration.ofMillis(1)
            );
        }

        private TestOutputTopic<String, MessageEnvelope> output(TopologyTestDriver driver) {
            return driver.createOutputTopic(
                    settings.outputTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.envelopeSerde().deserializer()
            );
        }

        private TestOutputTopic<String, DbSyncEnvelope> dbSyncOutput(TopologyTestDriver driver) {
            return driver.createOutputTopic(
                    settings.dbSyncTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.dbSyncEnvelopeSerde().deserializer()
            );
        }
    }
}
