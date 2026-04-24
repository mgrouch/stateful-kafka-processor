package com.example.stateful.processor.topology;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.StateStores;
import com.example.stateful.processor.state.TBucket;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyFactoryTest {

    @Test
    void newTWithoutMatchingSStaysUnprocessed() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L, 0L)), t0.toEpochMilli());

            assertThat(output.isEmpty()).isTrue();
            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStores.UNPROCESSED_T_STORE);
            TBucket bucket = tStore.get("IBM");
            assertThat(bucket.items()).hasSize(1);
            assertThat(bucket.items().get(0).q_a()).isZero();
        }
    }

    @Test
    void newSWithoutMatchingTStaysUnprocessed() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 25L, 0L)), t0.toEpochMilli());

            KeyValueStore<String, SBucket> sStore = driver.getKeyValueStore(StateStores.UNPROCESSED_S_STORE);
            SBucket bucket = sStore.get("IBM");
            assertThat(bucket.items()).hasSize(1);
            assertThat(bucket.items().get(0).q_a()).isZero();
        }
    }

    @Test
    void newTPartiallyAllocatedByMatchingSRemainsOpen() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 40L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a()).isEqualTo(40L);
            T t = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items().get(0);
            assertThat(t.q_a()).isEqualTo(40L);
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("IBM").items()).isEmpty();
        }
    }

    @Test
    void newTFullyAllocatedByMatchingSRemovedFromUnprocessedT() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 120L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a()).isEqualTo(100L);
            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items()).isEmpty();
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("IBM").items().get(0).q_a()).isEqualTo(100L);
        }
    }

    @Test
    void newSPartiallyMatchesExistingTsAndOnlyClosedTsAreRemoved() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 30L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-2", "IBM", "R-2", false, 30L, 0L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 40L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(2);
            assertThat(emitted).extracting(v -> v.ts().q_a()).containsExactly(30L, 10L);

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items();
            assertThat(openT).hasSize(1);
            assertThat(openT.get(0).id()).isEqualTo(emitted.get(1).ts().tid());
            assertThat(openT.get(0).q_a()).isEqualTo(10L);
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("IBM").items()).isEmpty();
        }
    }

    @Test
    void newSPrioritizesFailStatusThenUsesDeterministicIdTieBreak() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-b", "IBM", "R-1", false, 30L, 0L, AStatus.NORMAL)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-a", "IBM", "R-2", false, 30L, 0L, AStatus.NORMAL)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-fail", "IBM", "R-3", false, 30L, 0L, AStatus.FAIL)), t0.plusMillis(2).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 70L, 0L, true)), t0.plusMillis(3).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(3);
            assertThat(emitted.get(0).ts().tid()).isEqualTo("t-fail");
            assertThat(emitted).extracting(v -> v.ts().q_a()).containsExactly(30L, 30L, 10L);

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items();
            assertThat(openT).hasSize(1);
            assertThat(openT.get(0).id()).isEqualTo(emitted.get(2).ts().tid());
            assertThat(openT.get(0).q_a()).isEqualTo(10L);

            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("IBM").items()).isEmpty();
        }
    }

    @Test
    void sameSeedProducesSameLotteryOrder() throws Exception {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        List<String> first = emittedTidForSeed(1234L, t0);
        List<String> second = emittedTidForSeed(1234L, t0.plusSeconds(5));
        assertThat(first).containsExactlyElementsOf(second);
    }

    @Test
    void differentSeedsCanProduceDifferentLotteryOrderWithinPriorityBucket() throws Exception {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        List<String> first = emittedTidForSeed(111L, t0);
        List<String> second = emittedTidForSeed(222L, t0.plusSeconds(5));
        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void rolloverSupplyIsConsumedBeforeNonRollover() throws Exception {
        TestHarness harness = new TestHarness(77L);
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-normal", "IBM", 50L, 0L, false)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-roll", "IBM", 50L, 0L, true)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 60L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(2);
            assertThat(emitted).extracting(v -> v.ts().sid()).containsExactly("s-roll", "s-normal");
        }
    }

    @Test
    void negativeToNegativeAllocationWorks() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", -40L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-N", false, -100L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a()).isEqualTo(-40L);
            T t = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items().get(0);
            assertThat(t.q_a()).isEqualTo(-40L);
        }
    }

    @Test
    void positiveAndNegativeDoNotCrossAllocate() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-neg", "IBM", -30L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-pos", "IBM", "R-P", false, 50L, 0L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-pos", "IBM", 30L, 0L)), t0.plusMillis(2).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-neg", "IBM", "R-N", false, -50L, 0L)), t0.plusMillis(3).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(2);
            assertThat(emitted).extracting(v -> v.ts().q_a()).containsExactly(30L, -30L);
            assertThat(emitted).extracting(v -> v.ts().tid()).containsExactly("t-pos", "t-neg");
            assertThat(emitted).extracting(v -> v.ts().sid()).containsExactly("s-pos", "s-neg");
        }
    }

    @Test
    void signedPartialAllocationLeavesNegativeOpenQuantity() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", -60L, 0L)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-N", false, -100L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a()).isEqualTo(-60L);
            T open = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items().get(0);
            assertThat(open.q()).isEqualTo(-100L);
            assertThat(open.q_a()).isEqualTo(-60L);
        }
    }

    @Test
    void signedRolloverCarryIsConsumedBeforeRegularSupply() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-roll", "IBM", -10L, -40L, 0L, true)), t0.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, -25L, 0L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-2", "IBM", "R-2", false, -20L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).extracting(v -> v.ts().q_a()).containsExactly(-25L, -20L);
            S remaining = driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("IBM").items().get(0);
            assertThat(remaining.q()).isEqualTo(-10L);
            assertThat(remaining.q_carry()).isEqualTo(-40L);
            assertThat(remaining.q_a()).isEqualTo(-45L);
        }
    }

    @Test
    void tDedupeStillUsesPidRefCancel() throws Exception {
        TestHarness harness = new TestHarness();

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long withinWindow = t0.plus(Duration.ofDays(13)).toEpochMilli();

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 100L, 0L)), t0.toEpochMilli());
            inputTopic.pipeInput("IBM", MessageEnvelope.forT(new T("t-2", "IBM", "R-1", false, 101L, 0L)), withinWindow);

            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("IBM").items())
                    .extracting(T::id)
                    .containsExactly("t-1");
        }
    }

    @Test
    void invalidAllocationStateFailsFast() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            assertThatThrownBy(() -> input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 1L, 2L)), t0.toEpochMilli()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void topologyContainsOnlyExpectedStateStores() throws Exception {
        TestHarness harness = new TestHarness();
        Topology topology = TopologyFactory.create(harness.settings, harness.serdeFactory);
        String description = TopologyFactory.describe(topology);
        assertThat(description).contains(StateStores.UNPROCESSED_T_STORE, StateStores.UNPROCESSED_S_STORE, StateStores.T_DEDUPE_STORE);
    }

    @Test
    void modelHasRolloverOnSAndNoRolloverOnT() {
        assertThat(Arrays.stream(S.class.getRecordComponents()).map(c -> c.getName())).contains("rollover");
        assertThat(Arrays.stream(T.class.getRecordComponents()).map(c -> c.getName())).doesNotContain("rollover");
        assertThat(Arrays.stream(T.class.getRecordComponents()).map(c -> c.getName())).doesNotContain("q_carry");
        assertThat(Arrays.stream(S.class.getRecordComponents()).map(c -> c.getName())).contains("q_carry");
    }

    private static final class TestHarness {
        private final SerdeFactory serdeFactory;
        private final ProcessorSettings settings;

        private TestHarness() throws Exception {
            this(1357911L);
        }

        private TestHarness(long lotterySeed) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            this.serdeFactory = new SerdeFactory(mapper);
            this.settings = new ProcessorSettings(
                    "dummy:9092",
                    "SSL",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "topology-test-app",
                    "input-events",
                    "processed-events",
                    "db-sync-events",
                    Files.createTempDirectory("streams-test-state"),
                    100,
                    1,
                    0,
                    lotterySeed
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

        @SuppressWarnings("unused")
        private TestOutputTopic<String, DbSyncEnvelope> dbSyncOutput(TopologyTestDriver driver) {
            return driver.createOutputTopic(
                    settings.dbSyncTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.dbSyncEnvelopeSerde().deserializer()
            );
        }
    }

    private static List<String> emittedTidForSeed(long seed, Instant start) throws Exception {
        TestHarness harness = new TestHarness(seed);
        try (TopologyTestDriver driver = harness.driver(start)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, start);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-1", "IBM", "R-1", false, 20L, 0L, AStatus.NORMAL)), start.toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-2", "IBM", "R-2", false, 20L, 0L, AStatus.NORMAL)), start.plusMillis(1).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forT(new T("t-3", "IBM", "R-3", false, 20L, 0L, AStatus.NORMAL)), start.plusMillis(2).toEpochMilli());
            input.pipeInput("IBM", MessageEnvelope.forS(new S("s-1", "IBM", 45L, 0L, false)), start.plusMillis(3).toEpochMilli());

            return output.readValuesToList().stream().map(v -> v.ts().tid()).toList();
        }
    }
}
