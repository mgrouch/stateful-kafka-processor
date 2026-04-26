package com.example.stateful.processor.topology;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.Dir;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TT;
import com.example.stateful.messaging.DbSyncEnvelope;
import com.example.stateful.messaging.MessageEnvelope;
import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.state.SBucket;
import com.example.stateful.processor.state.StateStores;
import com.example.stateful.processor.state.TBucket;
import com.example.stateful.processor.logic.AllocationStrategy;
import com.example.stateful.processor.logic.AutoAllocOppositeStrategy;
import com.example.stateful.processor.logic.NaiveAlocationStrategy;
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
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.toEpochMilli());

            assertThat(output.isEmpty()).isTrue();
            KeyValueStore<String, TBucket> tStore = driver.getKeyValueStore(StateStores.UNPROCESSED_T_STORE);
            TBucket bucket = tStore.get("AAA");
            assertThat(bucket.items()).hasSize(1);
            assertThat(bucket.items().get(0).q_a_total()).isZero();
        }
    }

    @Test
    void newSWithoutMatchingTStaysUnprocessed() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 25L, 0L)), t0.toEpochMilli());

            KeyValueStore<String, SBucket> sStore = driver.getKeyValueStore(StateStores.UNPROCESSED_S_STORE);
            SBucket bucket = sStore.get("AAA");
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

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 40L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            MessageEnvelope ts = output.readValue();
            assertThat(ts.ts().q_a_delta()).isEqualTo(40L);
            assertThat(ts.ts().q_a_total_after()).isEqualTo(40L);
            assertThat(ts.ts().o()).isFalse();
            T t = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items().get(0);
            assertThat(t.q_a_total()).isEqualTo(40L);
            assertThat(t.q_a_delta_last()).isEqualTo(40L);
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items()).isEmpty();
        }
    }

    @Test
    void supplyOFlagPropagatesToGeneratedTs() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            S flaggedSupply = new S("s-o", "AAA", null, 40L, 0L, 0L, 0L, 0L, false, true, Dir.R, null);
            input.pipeInput("AAA", MessageEnvelope.forS(flaggedSupply), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            MessageEnvelope ts = output.readValue();
            assertThat(ts.ts().o()).isTrue();
        }
    }

    @Test
    void newTFullyAllocatedByMatchingSRemovedFromUnprocessedT() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 120L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a_total_after()).isEqualTo(100L);
            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items()).isEmpty();
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items().get(0).q_a()).isEqualTo(100L);
        }
    }

    @Test
    void newSPartiallyMatchesExistingTsAndOnlyClosedTsAreRemoved() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 30L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-2", "AAA", "R-2", false, 30L, 0L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 40L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(2);
            assertThat(emitted).extracting(v -> v.ts().q_a_delta()).containsExactly(30L, 10L);

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items();
            assertThat(openT).hasSize(1);
            assertThat(openT.get(0).id()).isEqualTo(emitted.get(1).ts().tid());
            assertThat(openT.get(0).q_a_total()).isEqualTo(10L);
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items()).isEmpty();
        }
    }

    @Test
    void newSPrioritizesFailStatusThenUsesDeterministicIdTieBreak() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-b", "AAA", "R-1", false, 30L, 0L, AStatus.NORM)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-a", "AAA", "R-2", false, 30L, 0L, AStatus.NORM)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-fail", "AAA", "R-3", null, TT.B, null, null, AStatus.FAIL, false, 30L, 0L, 0L, 5L)), t0.plusMillis(2).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 70L, 0L, true)), t0.plusMillis(3).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(3);
            assertThat(emitted.get(0).ts().tid()).isEqualTo("t-fail");
            assertThat(emitted).extracting(v -> v.ts().q_a_delta()).containsExactly(30L, 30L, 10L);

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items();
            assertThat(openT).hasSize(1);
            assertThat(openT.get(0).id()).isEqualTo(emitted.get(2).ts().tid());
            assertThat(openT.get(0).q_a_total()).isEqualTo(10L);

            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items()).isEmpty();
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
        List<List<String>> orders = List.of(
                emittedTidForSeed(111L, t0),
                emittedTidForSeed(222L, t0.plusSeconds(5)),
                emittedTidForSeed(333L, t0.plusSeconds(10)),
                emittedTidForSeed(444L, t0.plusSeconds(15))
        );
        assertThat(orders.stream().distinct().count()).isGreaterThan(1);
    }

    @Test
    void rolloverSupplyIsConsumedBeforeNonRollover() throws Exception {
        TestHarness harness = new TestHarness(77L);
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-normal", "AAA", 50L, 0L, false)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-roll", "AAA", 50L, 0L, true)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 60L, 0L)), t0.plusMillis(2).toEpochMilli());

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

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", -40L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-N", null, false, -100L, 0L, AStatus.NORM, TT.S)), t0.plusMillis(1).toEpochMilli());

            MessageEnvelope ts = output.readValue();
            assertThat(ts.ts().q_a_delta()).isEqualTo(-40L);
            assertThat(ts.ts().q_a_total_after()).isEqualTo(-40L);
            T t = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items().get(0);
            assertThat(t.q_a_total()).isEqualTo(-40L);
            assertThat(t.q_a_delta_last()).isEqualTo(-40L);
        }
    }

    @Test
    void positiveAndNegativeDoNotCrossAllocate() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-neg", "AAA", -30L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-pos", "AAA", "R-P", false, 50L, 0L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-pos", "AAA", 30L, 0L)), t0.plusMillis(2).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-neg", "AAA", "R-N", null, false, -50L, 0L, AStatus.NORM, TT.S)), t0.plusMillis(3).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(2);
            assertThat(emitted).extracting(v -> v.ts().q_a_delta()).containsExactly(30L, -30L);
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

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", -60L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-N", null, false, -100L, 0L, AStatus.NORM, TT.S)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.readValue().ts().q_a_total_after()).isEqualTo(-60L);
            T open = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items().get(0);
            assertThat(open.q()).isEqualTo(-100L);
            assertThat(open.q_a_total()).isEqualTo(-60L);
        }
    }

    @Test
    void signedRolloverCarryIsConsumedBeforeRegularSupply() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-roll", "AAA", -10L, -40L, 0L, true)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", null, false, -25L, 0L, AStatus.NORM, TT.S)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-2", "AAA", "R-2", null, false, -20L, 0L, AStatus.NORM, TT.S)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).extracting(v -> v.ts().q_a_delta()).containsExactly(-25L, -20L);
            S remaining = driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items().get(0);
            assertThat(remaining.q()).isEqualTo(-10L);
            assertThat(remaining.q_carry()).isEqualTo(-40L);
            assertThat(remaining.q_a()).isEqualTo(-45L);
        }
    }

    @Test
    void customAllocationStrategyCanBePluggedIn() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0, new AllocationStrategy() {
            @Override
            public long allocate(long targetOpen, long sourceOpen) {
                return 0L;
            }
        })) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 50L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 50L, 0L)), t0.plusMillis(1).toEpochMilli());

            assertThat(output.isEmpty()).isTrue();
            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items()).hasSize(1);
            assertThat(driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items()).hasSize(1);
        }
    }


    @Test
    void incomingRDirectionSDoesNotForceCloseNegativeTWithNaiveStrategy() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0, new NaiveAlocationStrategy(1357911L))) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-neg-1", "AAA", "R-N-1", null, false, -15L, 0L, AStatus.NORM, TT.S)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-neg-2", "AAA", "R-N-2", null, TT.S, null, null, AStatus.FAIL, false, -25L, -10L, 0L, -3L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-r", "AAA", 5L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).isEmpty();

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items();
            assertThat(openT).hasSize(2);
            assertThat(openT).extracting(T::id).containsExactly("t-neg-1", "t-neg-2");
            S openS = driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items().get(0);
            assertThat(openS.id()).isEqualTo("s-r");
            assertThat(openS.q_a()).isZero();
        }
    }

    @Test
    void autoAllocOppositeStrategyFullyAllocatesOppositeSignTAndUpdatesSDelta() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0, new AutoAllocOppositeStrategy(1357911L))) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-neg-open", "AAA", "R-N-1", null, false, -15L, -5L, AStatus.NORM, TT.S)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-neg-closed", "AAA", "R-N-2", null, false, -20L, -20L, AStatus.NORM, TT.S)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-pos", "AAA", "R-P-1", false, 8L, 0L)), t0.plusMillis(2).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-r", "AAA", null, 10L, 0L, 0L, 0L, 3L, false, Dir.R, null)), t0.plusMillis(3).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).extracting(v -> v.ts().tid()).containsExactly("t-neg-open", "t-pos");
            assertThat(emitted).extracting(v -> v.ts().q_a_delta()).containsExactly(-10L, 8L);
            assertThat(emitted).extracting(v -> v.ts().q_a_total_after()).containsExactly(-15L, 8L);

            S storedS = driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items().get(0);
            assertThat(storedS.q_a()).isEqualTo(8L);
            assertThat(storedS.q_a_opposite_delta()).isEqualTo(-10L);
            assertThat(storedS.q_a_opposite_total()).isEqualTo(-7L);

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items();
            assertThat(openT).extracting(T::id).doesNotContain("t-neg-open", "t-neg-closed", "t-pos");
        }
    }

    @Test
    void incomingDDirectionSDoesNotForceClosePositiveTWithNaiveStrategy() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0, new NaiveAlocationStrategy(1357911L))) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-pos-1", "AAA", "R-P-1", false, 30L, 0L)), t0.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-pos-2", "AAA", "R-P-2", false, 40L, 10L)), t0.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-d", "AAA", -5L, 0L)), t0.plusMillis(2).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).isEmpty();

            List<T> openT = driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items();
            assertThat(openT).hasSize(2);
            assertThat(openT).extracting(T::id).containsExactly("t-pos-1", "t-pos-2");
            S openS = driver.<String, SBucket>getKeyValueStore(StateStores.UNPROCESSED_S_STORE).get("AAA").items().get(0);
            assertThat(openS.id()).isEqualTo("s-d");
            assertThat(openS.q_a()).isZero();
        }
    }

    @Test
    void tDedupeStillUsesPidRefCancel() throws Exception {
        TestHarness harness = new TestHarness();

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        long withinWindow = t0.plus(Duration.ofDays(13)).toEpochMilli();

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            inputTopic.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.toEpochMilli());
            inputTopic.pipeInput("AAA", MessageEnvelope.forT(new T("t-2", "AAA", "R-1", false, 101L, 0L)), withinWindow);

            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items())
                    .extracting(T::id)
                    .containsExactly("t-1");
        }
    }

    @Test
    void cancelTClosesMatchingUnprocessedTWithZeroAllocationAndEmitsCancelTs() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> inputTopic = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);
            inputTopic.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 100L, 0L)), t0.toEpochMilli());
            inputTopic.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", true, 100L, 0L)), t0.plusMillis(1).toEpochMilli());

            List<MessageEnvelope> emitted = output.readValuesToList();
            assertThat(emitted).hasSize(1);
            assertThat(emitted.get(0).ts().tid()).isEqualTo("t-1");
            assertThat(emitted.get(0).ts().q_a_delta()).isEqualTo(100L);
            assertThat(emitted.get(0).ts().q_a_total_after()).isEqualTo(100L);
            assertThat(emitted.get(0).ts().cancel()).isTrue();
            assertThat(driver.<String, TBucket>getKeyValueStore(StateStores.UNPROCESSED_T_STORE).get("AAA").items()).isEmpty();
        }
    }

    @Test
    void invalidAllocationStateFailsFast() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            assertThatThrownBy(() -> input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 1L, 2L)), t0.toEpochMilli()))
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

    @Test
    void incomingFailedTIsForwardedToFailedTSink() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> failedTOutput = harness.failedTOutput(driver);

            T failedT = new T("t-failed", "AAA", "R-failed", null, null, false, 100L, 0L, 0L, 5L, AStatus.FAIL, TT.B);
            input.pipeInput("AAA", MessageEnvelope.forT(failedT), t0.toEpochMilli());

            assertThat(failedTOutput.readValue()).isEqualTo(MessageEnvelope.forT(failedT));
        }
    }

    @Test
    void incomingSWithCarryIsForwardedToCarrySink() throws Exception {
        TestHarness harness = new TestHarness();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        try (TopologyTestDriver driver = harness.driver(t0)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, t0);
            TestOutputTopic<String, MessageEnvelope> carryOutput = harness.sWithQCarryOutput(driver);

            S withCarry = new S("s-carry", "AAA", 100L, 25L, 0L, false);
            input.pipeInput("AAA", MessageEnvelope.forS(withCarry), t0.toEpochMilli());

            assertThat(carryOutput.readValue()).isEqualTo(MessageEnvelope.forS(withCarry));
        }
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
                    "failed-t-events",
                    "s-with-q-carry-events",
                    Files.createTempDirectory("streams-test-state"),
                    100,
                    1,
                    0,
                    lotterySeed
            );
        }

        private TopologyTestDriver driver(Instant t0) {
            return driver(t0, new AutoAllocOppositeStrategy(settings.allocationLotterySeed()));
        }

        private TopologyTestDriver driver(Instant t0, AllocationStrategy allocationStrategy) {
            Topology topology = TopologyFactory.create(settings, serdeFactory, allocationStrategy);
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

        private TestOutputTopic<String, MessageEnvelope> failedTOutput(TopologyTestDriver driver) {
            return driver.createOutputTopic(
                    settings.failedTTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.envelopeSerde().deserializer()
            );
        }

        private TestOutputTopic<String, MessageEnvelope> sWithQCarryOutput(TopologyTestDriver driver) {
            return driver.createOutputTopic(
                    settings.sWithQCarryTopic(),
                    Serdes.String().deserializer(),
                    serdeFactory.envelopeSerde().deserializer()
            );
        }
    }

    private static List<String> emittedTidForSeed(long seed, Instant start) throws Exception {
        TestHarness harness = new TestHarness(seed);
        try (TopologyTestDriver driver = harness.driver(start)) {
            TestInputTopic<String, MessageEnvelope> input = harness.input(driver, start);
            TestOutputTopic<String, MessageEnvelope> output = harness.output(driver);

            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-1", "AAA", "R-1", false, 20L, 0L, AStatus.NORM)), start.toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-2", "AAA", "R-2", false, 20L, 0L, AStatus.NORM)), start.plusMillis(1).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forT(new T("t-3", "AAA", "R-3", false, 20L, 0L, AStatus.NORM)), start.plusMillis(2).toEpochMilli());
            input.pipeInput("AAA", MessageEnvelope.forS(new S("s-1", "AAA", 45L, 0L, false)), start.plusMillis(3).toEpochMilli());

            return output.readValuesToList().stream().map(v -> v.ts().tid()).toList();
        }
    }
}
