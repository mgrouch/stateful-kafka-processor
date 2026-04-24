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

import static org.assertj.core.api.Assertions.assertThat;

class TopologyFactoryTest {

    @Test
    void tInputEmitsProcessedAndDbSyncEventsInStableOrder() throws Exception {
        SerdeFactory serdeFactory = serdeFactory();
        ProcessorSettings settings = settings("topology-test-app");

        Topology topology = TopologyFactory.create(settings, serdeFactory);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, settings.toStreamsProperties(), t0)) {
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
            TestOutputTopic<String, DbSyncEnvelope> dbSyncTopic = driver.createOutputTopic(
                    settings.dbSyncTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.dbSyncEnvelopeSerde().deserializer()
            );

            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L)), t0.toEpochMilli());

            assertThat(outputTopic.readValue().ts().id()).isEqualTo("ts-t-1");
            assertThat(dbSyncTopic.readValuesToList())
                    .extracting(DbSyncEnvelope::mutationType)
                    .containsExactly(
                            DbSyncMutationType.ACCEPTED_T,
                            DbSyncMutationType.UPSERT_UNPROCESSED_T,
                            DbSyncMutationType.GENERATED_TS
                    );
            assertThat(dbSyncTopic.readValuesToList()).isEmpty();
        }
    }

    @Test
    void sInputEmitsAcceptedAndUpsertStateDbSyncEvents() throws Exception {
        SerdeFactory serdeFactory = serdeFactory();
        ProcessorSettings settings = settings("topology-test-app-s");

        Topology topology = TopologyFactory.create(settings, serdeFactory);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, settings.toStreamsProperties(), t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = driver.createInputTopic(
                    settings.inputTopic(),
                    Serdes.String().serializer(),
                    serdeFactory.envelopeSerde().serializer()
            );
            TestOutputTopic<String, DbSyncEnvelope> dbSyncTopic = driver.createOutputTopic(
                    settings.dbSyncTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.dbSyncEnvelopeSerde().deserializer()
            );

            inputTopic.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 55L)), t0.toEpochMilli());

            assertThat(dbSyncTopic.readValuesToList())
                    .extracting(DbSyncEnvelope::mutationType)
                    .containsExactly(DbSyncMutationType.ACCEPTED_S, DbSyncMutationType.UPSERT_UNPROCESSED_S);
        }
    }

    @Test
    void tDeduplicationUsesPidRefCancelWithTwoWeekWindow() throws Exception {
        SerdeFactory serdeFactory = serdeFactory();
        ProcessorSettings settings = settings("topology-test-app-dedupe");

        Topology topology = TopologyFactory.create(settings, serdeFactory);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long withinWindow = t0.plus(Duration.ofDays(13)).toEpochMilli();
        long outsideWindow = t0.plus(Duration.ofDays(15)).toEpochMilli();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, settings.toStreamsProperties(), t0)) {
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

            assertThat(outputTopic.readKeyValuesToList())
                    .extracting(kv -> kv.key, kv -> kv.value.ts().id())
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-1"),
                            org.assertj.core.groups.Tuple.tuple("IBM", "ts-t-3")
                    );

            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStoresConfig.UNPROCESSED_T_STORE);
            assertThat(tStore.get("IBM").items()).extracting(T::id).containsExactly("t-1", "t-3");
        }
    }

    @Test
    void topologyContainsOnlyProcessingStateStores() throws Exception {
        Topology topology = TopologyFactory.create(settings("topology-test-app-stores"), serdeFactory());
        String description = TopologyFactory.describe(topology);

        assertThat(description).contains(StateStoresConfig.UNPROCESSED_T_STORE, StateStoresConfig.UNPROCESSED_S_STORE, StateStoresConfig.T_DEDUPE_STORE);
        assertThat(description).doesNotContain("processedT").doesNotContain("processedS");
    }

    private static SerdeFactory serdeFactory() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return new SerdeFactory(mapper);
    }

    private static ProcessorSettings settings(String appId) throws Exception {
        return new ProcessorSettings(
                "dummy:9092",
                appId,
                "input-events",
                "processed-events",
                "db-sync-events",
                Files.createTempDirectory("streams-test-state"),
                100
        );
    }
}
