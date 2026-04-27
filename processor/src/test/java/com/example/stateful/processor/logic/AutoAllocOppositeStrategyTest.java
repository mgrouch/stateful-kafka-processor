package com.example.stateful.processor.logic;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.Dir;
import com.example.stateful.domain.S;
import com.example.stateful.domain.SMode;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.domain.TT;
import com.example.stateful.domain.TCycle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class AutoAllocOppositeStrategyTest {

    @Test
    void orderTCandidatesForIncomingSFollowsRequestedPriorityBuckets() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(1357911L);
        S incomingS = new S("s-1", "PID", 100L, 0L);

        List<T> candidates = List.of(
                t("g8-other-open", TT.B, 10L, 0L, 0L, AStatus.NORM),
                t("g7-open-s", TT.S, 10L, 0L, 0L, AStatus.NORM),
                t("g3-fully-failed-s", TT.S, 10L, 0L, 2L, AStatus.FAIL),
                t("g6-partial-alloc-non-s", TT.CS, 10L, 3L, 0L, AStatus.NORM),
                t("g4-fully-failed-non-s", TT.B, 10L, 0L, 2L, AStatus.FAIL),
                t("g1-partial-failed-s", TT.S, 10L, 2L, 1L, AStatus.FAIL),
                t("g2-partial-failed-ss", TT.SS, 10L, 2L, 1L, AStatus.FAIL),
                t("g2-partial-failed-cs", TT.CS, 10L, 2L, 1L, AStatus.FAIL),
                t("g2-partial-failed-b", TT.B, 10L, 2L, 1L, AStatus.FAIL),
                t("g5-partial-alloc-s", TT.S, 10L, 3L, 0L, AStatus.NORM)
        );

        List<T> ordered = strategy.orderTCandidatesForIncomingS(candidates, incomingS);

        assertThat(ordered).extracting(T::id).containsExactly(
                "g1-partial-failed-s",
                "g2-partial-failed-b",
                "g2-partial-failed-ss",
                "g2-partial-failed-cs",
                "g3-fully-failed-s",
                "g4-fully-failed-non-s",
                "g5-partial-alloc-s",
                "g6-partial-alloc-non-s",
                "g7-open-s",
                "g8-other-open"
        );
    }

    @Test
    void orderTCandidatesForIncomingSIsDeterministicWithinBucketLottery() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", 100L, 0L);

        List<T> candidates = List.of(
                t("pf-s-1", TT.S, 10L, 2L, 1L, AStatus.FAIL),
                t("pf-s-2", TT.S, 10L, 2L, 1L, AStatus.FAIL),
                t("pf-s-3", TT.S, 10L, 2L, 1L, AStatus.FAIL)
        );

        List<String> firstOrder = strategy.orderTCandidatesForIncomingS(candidates, incomingS).stream().map(T::id).toList();
        List<String> secondOrder = strategy.orderTCandidatesForIncomingS(candidates, incomingS).stream().map(T::id).toList();

        assertThat(firstOrder).containsExactlyElementsOf(secondOrder);
    }

    private static T t(String id, TT tt, long q, long qATotal, long qF, AStatus status) {
        return t(id, tt, q, qATotal, qF, status, TCycle.SD, null);
    }


    @Test
    void orderTCandidatesForIncomingSPrioritizesSdBeforeRtAndUsesFifoWithinRt() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", 100L, 0L);

        List<T> candidates = List.of(
                t("rt-2", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.RT, 200L),
                t("sd-1", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.SD, 150L),
                t("rt-1", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.RT, 100L)
        );

        List<String> order = strategy.orderTCandidatesForIncomingS(candidates, incomingS).stream().map(T::id).toList();

        assertThat(order).containsExactly("sd-1", "rt-1", "rt-2");
    }

    @Test
    void allocateForIncomingSOnlyAllocatesWhenTLedgerTimeIsBeforeOrEqualToSLedgerTime() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        T eligible = t("t-eligible", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L);
        T ineligible = t("t-ineligible", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 101L);

        AllocationResult result = strategy.allocateForIncomingS(
                List.of(eligible, ineligible),
                List.of(),
                incomingS,
                "ts");

        assertThat(result.updatedIncomingS().q_a()).isEqualTo(10L);
        assertThat(result.updatedT())
                .extracting(T::id, T::q_a_total)
                .containsExactly(
                        tuple("t-eligible", 10L),
                        tuple("t-ineligible", 0L)
                );
    }

    @Test
    void allocateForIncomingSOnlyAutoAllocatesOppositeWhenLedgerTimeIsCompatible() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        T eligibleOpposite = t("t-opposite-eligible", TT.S, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 99L);
        T ineligibleOpposite = t("t-opposite-ineligible", TT.S, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 101L);

        AllocationResult result = strategy.allocateForIncomingS(
                List.of(eligibleOpposite, ineligibleOpposite),
                List.of(),
                incomingS,
                "ts");

        assertThat(result.updatedIncomingS().q_a_opposite_delta()).isEqualTo(-10L);
        assertThat(result.updatedT())
                .extracting(T::id, T::q_a_total)
                .containsExactly(
                        tuple("t-opposite-ineligible", 0L),
                        tuple("t-opposite-eligible", -10L)
                );
    }

    @Test
    void allocateForIncomingSOnlyAutoAllocatesOppositeForCnMode() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        T cnModeOpposite = t("t-opposite-cn", TT.S, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 99L, SMode.CN);
        T csModeOpposite = t("t-opposite-cs", TT.S, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 99L, SMode.CS);

        AllocationResult result = strategy.allocateForIncomingS(
                List.of(cnModeOpposite, csModeOpposite),
                List.of(),
                incomingS,
                "ts");

        assertThat(result.updatedIncomingS().q_a_opposite_delta()).isEqualTo(-10L);
        assertThat(result.updatedT())
                .extracting(T::id, T::q_a_total)
                .containsExactly(
                        tuple("t-opposite-cs", 0L),
                        tuple("t-opposite-cn", -10L)
                );
    }

    @Test
    void allocateForIncomingSIgnoresCandidatesWithDifferentSMode() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        T matchingMode = t("t-match", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CN);
        T differentMode = t("t-mismatch", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CS);

        AllocationResult result = strategy.allocateForIncomingS(
                List.of(matchingMode, differentMode),
                List.of(),
                incomingS,
                "ts");

        assertThat(result.updatedIncomingS().q_a()).isEqualTo(10L);
        assertThat(result.updatedT())
                .extracting(T::id, T::q_a_total)
                .containsExactly(
                        tuple("t-match", 10L),
                        tuple("t-mismatch", 0L)
                );
    }

    @Test
    void allocateForIncomingTAutoAllocatesOppositeSBeforeRegularAllocation() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        T incomingT = t("t-incoming", TT.B, 15L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CN);
        S oppositeS = new S("s-opposite", "PID", null, -12L, 0L, 0L, 0L, 5L, false, false, Dir.D, 99L);
        S regularS = new S("s-regular", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        AllocationResult result = strategy.allocateForIncomingT(
                incomingT,
                List.of(oppositeS, regularS),
                List.of(),
                "ts");

        assertThat(result.updatedIncomingT().q_a_total()).isEqualTo(15L);
        assertThat(result.updatedS())
                .extracting(S::id, S::q_a, S::q_a_opposite_delta, S::q_a_opposite_total)
                .containsExactly(
                        tuple("s-regular", 3L, 0L, 0L),
                        tuple("s-opposite", 0L, 12L, 17L)
                );
        assertThat(result.emittedTs())
                .extracting(TS::sid, TS::q_a_delta, TS::q_a_total_after)
                .containsExactly(
                        tuple("s-opposite", 12L, 12L),
                        tuple("s-regular", 3L, 15L)
                );
    }

    @Test
    void allocateForIncomingTOnlyAutoAllocatesOppositeWhenLedgerTimeIsCompatible() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        T incomingT = t("t-incoming", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CN);
        S eligibleOpposite = new S("s-opposite-eligible", "PID", null, -8L, 0L, 0L, 0L, 0L, false, false, Dir.D, 100L);
        S ineligibleOpposite = new S("s-opposite-ineligible", "PID", null, -8L, 0L, 0L, 0L, 0L, false, false, Dir.D, 101L);

        AllocationResult result = strategy.allocateForIncomingT(
                incomingT,
                List.of(eligibleOpposite, ineligibleOpposite),
                List.of(),
                "ts");

        assertThat(result.updatedIncomingT().q_a_total()).isEqualTo(8L);
        assertThat(result.updatedS())
                .extracting(S::id, S::q_a_opposite_delta)
                .containsExactly(
                        tuple("s-opposite-ineligible", 0L),
                        tuple("s-opposite-eligible", 8L)
                );
    }

    @Test
    void allocateForIncomingTOnlyAutoAllocatesOppositeForCnMode() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        T incomingT = t("t-incoming", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CS);
        S oppositeS = new S("s-opposite", "PID", null, -10L, 0L, 0L, 0L, 0L, false, true, Dir.D, 100L);

        AllocationResult result = strategy.allocateForIncomingT(
                incomingT,
                List.of(oppositeS),
                List.of(),
                "ts");

        assertThat(result.updatedIncomingT().q_a_total()).isEqualTo(0L);
        assertThat(result.updatedS())
                .extracting(S::id, S::q_a_opposite_delta)
                .containsExactly(tuple("s-opposite", 0L));
    }


    @Test
    void allocateForIncomingSIgnoresIncomingSWithOTrue() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-incoming", "PID", null, 10L, 0L, 0L, 0L, 0L, false, true, Dir.R, 100L);
        T candidate = t("t-match", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CN);

        AllocationResult result = strategy.allocateForIncomingS(
                List.of(candidate),
                List.of(),
                incomingS,
                "ts");

        assertThat(result.updatedIncomingS().q_a()).isEqualTo(0L);
        assertThat(result.updatedT()).extracting(T::id, T::q_a_total).containsExactly(tuple("t-match", 0L));
        assertThat(result.emittedTs()).isEmpty();
    }

    @Test
    void allocateForIncomingTIgnoresCandidatesWithOTrue() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        T incomingT = t("t-incoming", TT.B, 10L, 0L, 0L, AStatus.NORM, TCycle.SD, 100L, SMode.CN);
        S ineligible = new S("s-o-true", "PID", null, 10L, 0L, 0L, 0L, 0L, false, true, Dir.R, 100L);
        S eligible = new S("s-o-false", "PID", null, 10L, 0L, 0L, 0L, 0L, false, false, Dir.R, 100L);

        AllocationResult result = strategy.allocateForIncomingT(
                incomingT,
                List.of(ineligible, eligible),
                List.of(),
                "ts");

        assertThat(result.updatedIncomingT().q_a_total()).isEqualTo(10L);
        assertThat(result.updatedS())
                .extracting(S::id, S::q_a)
                .containsExactly(
                        tuple("s-o-false", 10L),
                        tuple("s-o-true", 0L)
                );
        assertThat(result.emittedTs()).extracting(TS::sid).containsExactly("s-o-false");
    }

    private static T t(String id, TT tt, long q, long qATotal, long qF, AStatus status, TCycle tCycle, Long ledgerTime) {
        return t(id, tt, q, qATotal, qF, status, tCycle, ledgerTime, null);
    }

    private static T t(String id, TT tt, long q, long qATotal, long qF, AStatus status, TCycle tCycle, Long ledgerTime, SMode sMode) {
        long normalizedQ = (tt == TT.S || tt == TT.SS) ? -Math.abs(q) : Math.abs(q);
        long normalizedQATotal = qATotal == 0L ? 0L : Long.signum(normalizedQ) * Math.abs(qATotal);
        return new T(id, "PID", "REF-" + id, null, tt, null, null, tCycle, sMode, status, false, normalizedQ, normalizedQATotal, 0L, qF, ledgerTime);
    }
}
