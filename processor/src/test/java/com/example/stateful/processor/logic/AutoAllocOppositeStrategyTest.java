package com.example.stateful.processor.logic;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TT;
import com.example.stateful.domain.TCycle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
    void orderTCandidatesForIncomingSUsesFifoForRtWithinBucketByLedgerTime() {
        AutoAllocOppositeStrategy strategy = new AutoAllocOppositeStrategy(24680L);
        S incomingS = new S("s-1", "PID", 100L, 0L);

        List<T> candidates = List.of(
                t("rt-2", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.RT, 200L),
                t("sd-1", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.SD, 150L),
                t("rt-1", TT.S, 10L, 2L, 1L, AStatus.FAIL, TCycle.RT, 100L)
        );

        List<String> order = strategy.orderTCandidatesForIncomingS(candidates, incomingS).stream().map(T::id).toList();

        assertThat(order.subList(0, 2)).containsExactly("rt-1", "rt-2");
    }
    private static T t(String id, TT tt, long q, long qATotal, long qF, AStatus status, TCycle tCycle, Long ledgerTime) {
        long normalizedQ = (tt == TT.S || tt == TT.SS) ? -Math.abs(q) : Math.abs(q);
        long normalizedQATotal = qATotal == 0L ? 0L : Long.signum(normalizedQ) * Math.abs(qATotal);
        return new T(id, "PID", "REF-" + id, null, tt, null, null, tCycle, null, status, false, normalizedQ, normalizedQATotal, 0L, qF, ledgerTime);
    }
}
