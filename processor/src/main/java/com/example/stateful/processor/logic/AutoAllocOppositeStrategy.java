package com.example.stateful.processor.logic;

import com.example.stateful.domain.S;
import com.example.stateful.domain.SMode;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.example.stateful.domain.TT;
import com.example.stateful.domain.TCycle;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public final class AutoAllocOppositeStrategy implements AllocationStrategy {

    private static final String LOTTERY_INCOMING_S = "INCOMING_S";
    private static final String LOTTERY_PARTIAL_FAIL_S_BUCKET = "PARTIAL_FAIL_S";
    private static final String LOTTERY_PARTIAL_FAIL_NON_S_BUCKET = "PARTIAL_FAIL_NON_S";
    private static final String LOTTERY_FULL_FAIL_S_BUCKET = "FULL_FAIL_S";
    private static final String LOTTERY_FULL_FAIL_NON_S_BUCKET = "FULL_FAIL_NON_S";
    private static final String LOTTERY_PARTIAL_ALLOC_S_BUCKET = "PARTIAL_ALLOC_S";
    private static final String LOTTERY_PARTIAL_ALLOC_NON_S_BUCKET = "PARTIAL_ALLOC_NON_S";
    private static final String LOTTERY_OPEN_S_BUCKET = "OPEN_S";
    private static final String LOTTERY_OPEN_OTHER_BUCKET = "OPEN_OTHER";

    private final long allocationLotterySeed;

    public AutoAllocOppositeStrategy(long allocationLotterySeed) {
        this.allocationLotterySeed = allocationLotterySeed;
    }

    @Override
    public long allocate(long targetOpen, long sourceOpen) {
        if (!areSignCompatible(targetOpen, sourceOpen)) {
            return 0L;
        }
        long magnitude = Math.min(Math.abs(targetOpen), Math.abs(sourceOpen));
        return Long.signum(targetOpen) * magnitude;
    }

    @Override
    public List<S> orderSCandidatesForIncomingT(List<S> candidates, T incomingT) {
        return candidates.stream()
                .sorted(Comparator
                        .comparing((S s) -> !s.rollover())
                        .thenComparing(S::id))
                .toList();
    }

    @Override
    public List<T> orderTCandidatesForIncomingS(List<T> candidates, S incomingS) {
        List<T> canonical = candidates.stream().sorted(Comparator.comparing(T::id)).toList();

        List<T> ordered = new ArrayList<>(candidates.size());
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isPartiallyFailedS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_PARTIAL_FAIL_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isPartiallyFailedNonS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_PARTIAL_FAIL_NON_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isFullyFailedS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_FULL_FAIL_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isFullyFailedNonS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_FULL_FAIL_NON_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isPartiallyAllocatedS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_PARTIAL_ALLOC_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isPartiallyAllocatedNonS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_PARTIAL_ALLOC_NON_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isOtherOpenS)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_OPEN_S_BUCKET));
        ordered.addAll(shuffleDeterministically(canonical.stream()
                        .filter(this::isOtherOpenTrade)
                        .toList(),
                incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_OPEN_OTHER_BUCKET));
        return ordered;
    }

    @Override
    public AllocationResult allocateForIncomingT(T incomingT, List<S> orderedCandidates, List<S> untouchedCandidates, String idPrefix) {
        long incomingTOpen = TransitionsModel.remainingT(incomingT);
        List<S> signCompatible = orderedCandidates.stream()
                .filter(candidate -> isSModeCompatible(candidate, incomingT))
                .filter(candidate -> isLedgerTimeCompatible(incomingT, candidate))
                .filter(candidate -> areSignCompatible(incomingTOpen, TransitionsModel.remainingS(candidate)))
                .toList();
        List<S> signIncompatible = orderedCandidates.stream()
                .filter(candidate -> !isSModeCompatible(candidate, incomingT)
                        || !isLedgerTimeCompatible(incomingT, candidate)
                        || !areSignCompatible(incomingTOpen, TransitionsModel.remainingS(candidate)))
                .toList();
        List<S> reorderedCompatible = orderSCandidatesForIncomingT(signCompatible, incomingT);
        List<S> combinedUntouched = new ArrayList<>(untouchedCandidates.size() + signIncompatible.size());
        combinedUntouched.addAll(untouchedCandidates);
        combinedUntouched.addAll(signIncompatible);
        return AllocationStrategy.super.allocateForIncomingT(incomingT, reorderedCompatible, combinedUntouched, idPrefix);
    }

    @Override
    public AllocationResult allocateForIncomingS(List<T> orderedCandidates, List<T> untouchedCandidates, S incomingS, String idPrefix) {
        List<T> regularOrdered = new ArrayList<>();
        List<T> regularUntouched = new ArrayList<>();
        List<T> updatedOpposite = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();

        long oppositeDelta = 0L;
        int tsIndex = 0;

        for (T candidate : orderedCandidates) {
            if (isOppositeAutoAllocationEligible(candidate, incomingS)) {
                long delta = candidate.q() - candidate.q_a_total();
                T nextT = copyWithAllocation(candidate, candidate.q(), delta, candidate.sDate());
                updatedOpposite.add(nextT);
                if (delta != 0L) {
                    emitted.add(new TS(idPrefix + "-" + (++tsIndex), incomingS.pid(), nextT.pidAlt1(), nextT.pidAlt2(), nextT.id(), incomingS.id(), nextT.accId(), nextT.sorId(), nextT.oarId(), nextT.tDate(), incomingS.bDate(), nextT.q(), delta, nextT.q_a_total(), nextT.tt(), incomingS.o()));
                }
                oppositeDelta += delta;
            } else {
                regularOrdered.add(candidate);
            }
        }

        for (T candidate : untouchedCandidates) {
            if (isOppositeAutoAllocationEligible(candidate, incomingS)) {
                long delta = candidate.q() - candidate.q_a_total();
                T nextT = copyWithAllocation(candidate, candidate.q(), delta, candidate.sDate());
                updatedOpposite.add(nextT);
                if (delta != 0L) {
                    emitted.add(new TS(idPrefix + "-" + (++tsIndex), incomingS.pid(), nextT.pidAlt1(), nextT.pidAlt2(), nextT.id(), incomingS.id(), nextT.accId(), nextT.sorId(), nextT.oarId(), nextT.tDate(), incomingS.bDate(), nextT.q(), delta, nextT.q_a_total(), nextT.tt(), incomingS.o()));
                }
                oppositeDelta += delta;
            } else {
                regularUntouched.add(candidate);
            }
        }

        S updatedS = new S(
                incomingS.id(),
                incomingS.pid(),
                incomingS.bDate(),
                incomingS.q(),
                incomingS.q_carry(),
                incomingS.q_a(),
                oppositeDelta,
                incomingS.q_a_opposite_total() + oppositeDelta,
                incomingS.rollover(),
                incomingS.o(),
                incomingS.dir(),
                incomingS.ledgerTime()
        );

        long incomingSOpen = TransitionsModel.remainingS(updatedS);
        S ledgerCheckedS = updatedS;
        List<T> signCompatible = regularOrdered.stream()
                .filter(candidate -> isSModeCompatible(ledgerCheckedS, candidate))
                .filter(candidate -> isLedgerTimeCompatible(candidate, ledgerCheckedS))
                .filter(candidate -> areSignCompatible(incomingSOpen, TransitionsModel.remainingT(candidate)))
                .toList();
        List<T> signIncompatible = regularOrdered.stream()
                .filter(candidate -> !isSModeCompatible(ledgerCheckedS, candidate)
                        || !isLedgerTimeCompatible(candidate, ledgerCheckedS)
                        || !areSignCompatible(incomingSOpen, TransitionsModel.remainingT(candidate)))
                .toList();
        List<T> reorderedCompatible = orderTCandidatesForIncomingS(signCompatible, updatedS);
        List<T> combinedUntouched = new ArrayList<>(regularUntouched.size() + signIncompatible.size());
        combinedUntouched.addAll(regularUntouched);
        combinedUntouched.addAll(signIncompatible);

        List<T> updatedT = new ArrayList<>();
        for (T candidate : reorderedCompatible) {
            long sRemaining = TransitionsModel.remainingS(updatedS);
            long tRemaining = TransitionsModel.remainingT(candidate);
            long allocated = allocate(sRemaining, tRemaining);

            if (allocated != 0L) {
                long nextTotal = candidate.q_a_total() + allocated;
                LocalDate allocatedSDate = nextTotal == candidate.q()
                        ? (updatedS.bDate() != null ? updatedS.bDate() : candidate.sDate())
                        : candidate.sDate();
                T nextT = copyWithAllocation(candidate, nextTotal, allocated, allocatedSDate);
                updatedS = new S(updatedS.id(), updatedS.pid(), updatedS.bDate(), updatedS.q(), updatedS.q_carry(), updatedS.q_a() + allocated, updatedS.q_a_opposite_delta(), updatedS.q_a_opposite_total(), updatedS.rollover(), updatedS.o(), updatedS.dir(), updatedS.ledgerTime());
                updatedT.add(nextT);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedS.pid(), nextT.pidAlt1(), nextT.pidAlt2(), nextT.id(), updatedS.id(), nextT.accId(), nextT.sorId(), nextT.oarId(), nextT.tDate(), updatedS.bDate(), nextT.q(), allocated, nextTotal, nextT.tt(), updatedS.o()));
            } else {
                updatedT.add(candidate);
            }
        }

        updatedT.addAll(combinedUntouched);
        updatedT.addAll(updatedOpposite);
        return new AllocationResult(null, List.of(), updatedS, updatedT, emitted);
    }

    private static boolean areSignCompatible(long lhs, long rhs) {
        return lhs != 0 && rhs != 0 && Long.signum(lhs) == Long.signum(rhs);
    }

    private static boolean hasOppositeSign(long lhs, long rhs) {
        return lhs != 0L && rhs != 0L && Long.signum(lhs) != Long.signum(rhs);
    }

    private static boolean isOppositeAutoAllocationEligible(T t, S s) {
        return t.sMode() == SMode.CN
                && hasOppositeSign(t.q(), s.q())
                && isLedgerTimeCompatible(t, s)
                && isSModeCompatible(s, t);
    }

    private static boolean isLedgerTimeCompatible(T t, S s) {
        return t.ledgerTime() == null || s.ledgerTime() == null || t.ledgerTime() <= s.ledgerTime();
    }

    private static boolean isSModeCompatible(S s, T t) {
        return t.sMode() == (s.o() ? SMode.CS : SMode.CN);
    }

    private static T copyWithAllocation(T source, long qATotal, long qADeltaLast, LocalDate sDate) {
        return new T(
                source.id(),
                source.pid(),
                source.pidAlt1(),
                source.pidAlt2(),
                source.ref(),
                source.accId(),
                source.sorId(),
                source.oarId(),
                source.tt(),
                source.tDate(),
                sDate,
                source.tCycle(),
                source.sMode(),
                source.a_status(),
                source.activity(),
                source.mStatus(),
                source.cancel(),
                source.q(),
                qATotal,
                qADeltaLast,
                source.q_f(),
                source.ledgerTime()
        );
    }

    private boolean isPartiallyFailedS(T candidate) {
        return candidate.q_a_total() != 0L && candidate.q_f() != 0L && candidate.tt() == TT.S;
    }

    private boolean isPartiallyFailedNonS(T candidate) {
        return candidate.q_a_total() != 0L && candidate.q_f() != 0L && candidate.tt() != TT.S;
    }

    private boolean isFullyFailedS(T candidate) {
        return candidate.q_a_total() == 0L && candidate.q_f() != 0L && candidate.tt() == TT.S;
    }

    private boolean isFullyFailedNonS(T candidate) {
        return candidate.q_a_total() == 0L && candidate.q_f() != 0L && candidate.tt() != TT.S;
    }

    private boolean isPartiallyAllocatedS(T candidate) {
        return candidate.q_a_total() != 0L && candidate.q_f() == 0L && candidate.tt() == TT.S;
    }

    private boolean isPartiallyAllocatedNonS(T candidate) {
        return candidate.q_a_total() != 0L && candidate.q_f() == 0L && candidate.tt() != TT.S;
    }

    private boolean isOtherOpenS(T candidate) {
        return candidate.q_a_total() == 0L && candidate.q_f() == 0L && candidate.tt() == TT.S;
    }

    private boolean isOtherOpenTrade(T candidate) {
        return !isPartiallyFailedS(candidate)
                && !isPartiallyFailedNonS(candidate)
                && !isFullyFailedS(candidate)
                && !isFullyFailedNonS(candidate)
                && !isPartiallyAllocatedS(candidate)
                && !isPartiallyAllocatedNonS(candidate)
                && !isOtherOpenS(candidate);
    }

    private List<T> shuffleDeterministically(List<T> bucket, String pid, String incomingId, String direction, String bucketName) {
        List<T> nonRt = bucket.stream()
                .filter(candidate -> candidate.tCycle() != TCycle.RT)
                .toList();

        List<T> fifoRt = bucket.stream()
                .filter(candidate -> candidate.tCycle() == TCycle.RT)
                .sorted(Comparator
                        .comparing(T::ledgerTime, Comparator.nullsLast(Long::compareTo))
                        .thenComparing(T::id))
                .toList();

        List<T> shuffled = new ArrayList<>(nonRt);
        long seed = deriveAllocationSeed(pid, incomingId, direction, bucketName);
        Collections.shuffle(shuffled, new Random(seed));

        List<T> ordered = new ArrayList<>(bucket.size());
        ordered.addAll(shuffled);
        ordered.addAll(fifoRt);
        return ordered;
    }

    private long deriveAllocationSeed(String pid, String incomingId, String direction, String bucketName) {
        return Objects.hash(allocationLotterySeed, pid, incomingId, direction, bucketName);
    }
}
