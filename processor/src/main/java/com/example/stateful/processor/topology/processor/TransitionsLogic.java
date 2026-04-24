package com.example.stateful.processor.topology.processor;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

final class TransitionsLogic {

    private static final String LOTTERY_INCOMING_S = "INCOMING_S";
    private static final String LOTTERY_FAIL_BUCKET = "FAIL";
    private static final String LOTTERY_NORMAL_BUCKET = "NORMAL";

    private final long allocationLotterySeed;

    TransitionsLogic(long allocationLotterySeed) {
        this.allocationLotterySeed = allocationLotterySeed;
    }

    AllocationResult allocateForIncomingT(T incomingT, List<S> candidates, String idPrefix) {
        requireSamePid(candidates.stream().map(S::pid).toList(), incomingT.pid(), "S candidate");
        long incomingTOpen = remainingT(incomingT);
        List<S> signCompatibleCandidates = candidates.stream()
                .filter(candidate -> areSignCompatible(incomingTOpen, remainingS(candidate)))
                .toList();
        List<S> untouchedCandidates = candidates.stream()
                .filter(candidate -> !areSignCompatible(incomingTOpen, remainingS(candidate)))
                .toList();
        List<S> orderedCandidates = orderSCandidates(signCompatibleCandidates);
        ensureRolloverPriority(orderedCandidates);

        List<S> updatedS = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        T updatedT = incomingT;
        int tsIndex = 0;

        for (S candidate : orderedCandidates) {
            long tRemaining = remainingT(updatedT);
            long sRemaining = remainingS(candidate);
            long allocated = signedAllocation(tRemaining, sRemaining);

            if (allocated != 0) {
                updatedT = new T(updatedT.id(), updatedT.pid(), updatedT.ref(), updatedT.cancel(), updatedT.q(), updatedT.q_a() + allocated, updatedT.a_status(), updatedT.tt());
                S nextS = new S(candidate.id(), candidate.pid(), candidate.q(), candidate.q_carry(), candidate.q_a() + allocated, candidate.rollover(), candidate.dir());
                updatedS.add(nextS);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedT.pid(), updatedT.id(), nextS.id(), updatedT.q(), allocated, updatedT.tt()));
            } else {
                updatedS.add(candidate);
            }
        }
        updatedS.addAll(untouchedCandidates);

        validateAllocationOutput(updatedT.pid(), List.of(updatedT), updatedS, emitted);
        return new AllocationResult(updatedT, updatedS, emitted);
    }

    AllocationResult allocateForIncomingS(List<T> candidates, S incomingS, String idPrefix) {
        long incomingSOpen = remainingS(incomingS);
        List<T> signCompatibleCandidates = candidates.stream()
                .filter(candidate -> areSignCompatible(incomingSOpen, remainingT(candidate)))
                .toList();
        List<T> untouchedCandidates = candidates.stream()
                .filter(candidate -> !areSignCompatible(incomingSOpen, remainingT(candidate)))
                .toList();
        List<T> orderedCandidates = orderTCandidates(signCompatibleCandidates, incomingS.pid(), incomingS.id());
        requireSamePid(orderedCandidates.stream().map(T::pid).toList(), incomingS.pid(), "T candidate");
        ensureFailPriority(orderedCandidates);

        List<T> updatedT = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        S updatedS = incomingS;
        int tsIndex = 0;

        for (T candidate : orderedCandidates) {
            long sRemaining = remainingS(updatedS);
            long tRemaining = remainingT(candidate);
            long allocated = signedAllocation(sRemaining, tRemaining);

            if (allocated != 0) {
                T nextT = new T(candidate.id(), candidate.pid(), candidate.ref(), candidate.cancel(), candidate.q(), candidate.q_a() + allocated, candidate.a_status(), candidate.tt());
                updatedS = new S(updatedS.id(), updatedS.pid(), updatedS.q(), updatedS.q_carry(), updatedS.q_a() + allocated, updatedS.rollover(), updatedS.dir());
                updatedT.add(nextT);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedS.pid(), nextT.id(), updatedS.id(), nextT.q(), allocated, nextT.tt()));
            } else {
                updatedT.add(candidate);
            }
        }
        updatedT.addAll(untouchedCandidates);

        validateAllocationOutput(updatedS.pid(), updatedT, List.of(updatedS), emitted);
        return new AllocationResult(null, List.of(), updatedS, updatedT, emitted);
    }

    boolean isOpen(T t) {
        return remainingT(t) != 0;
    }

    boolean isOpen(S s) {
        return remainingS(s) != 0;
    }

    private List<S> orderSCandidates(List<S> candidates) {
        return candidates.stream()
                .sorted(Comparator
                        .comparing((S s) -> !s.rollover())
                        .thenComparing(S::id))
                .toList();
    }

    private List<T> orderTCandidates(List<T> candidates, String pid, String incomingId) {
        List<T> canonical = candidates.stream().sorted(Comparator.comparing(T::id)).toList();
        List<T> fail = canonical.stream().filter(t -> t.a_status() == AStatus.FAIL).toList();
        List<T> normal = canonical.stream().filter(t -> t.a_status() != AStatus.FAIL).toList();

        List<T> ordered = new ArrayList<>(candidates.size());
        ordered.addAll(shuffleDeterministically(fail, pid, incomingId, LOTTERY_INCOMING_S, LOTTERY_FAIL_BUCKET));
        ordered.addAll(shuffleDeterministically(normal, pid, incomingId, LOTTERY_INCOMING_S, LOTTERY_NORMAL_BUCKET));
        return ordered;
    }

    private List<T> shuffleDeterministically(List<T> bucket, String pid, String incomingId, String direction, String bucketName) {
        List<T> shuffled = new ArrayList<>(bucket);
        long seed = deriveAllocationSeed(pid, incomingId, direction, bucketName);
        Collections.shuffle(shuffled, new Random(seed));
        return shuffled;
    }

    private long deriveAllocationSeed(String pid, String incomingId, String direction, String bucketName) {
        return Objects.hash(allocationLotterySeed, pid, incomingId, direction, bucketName);
    }

    private static void ensureFailPriority(List<T> orderedCandidates) {
        boolean seenNormal = false;
        for (T candidate : orderedCandidates) {
            if (candidate.a_status() == AStatus.FAIL) {
                if (seenNormal) {
                    throw new IllegalStateException("FAIL T must be ordered before NORMAL T");
                }
            } else {
                seenNormal = true;
            }
        }
    }

    private static void ensureRolloverPriority(List<S> orderedCandidates) {
        boolean seenNonRollover = false;
        for (S candidate : orderedCandidates) {
            if (candidate.rollover()) {
                if (seenNonRollover) {
                    throw new IllegalStateException("rollover S must be ordered before non-rollover S");
                }
            } else {
                seenNonRollover = true;
            }
        }
    }

    private void validateAllocationOutput(String pid, List<T> allowedT, List<S> allowedS, List<TS> emittedTs) {
        Set<String> allowedTid = new HashSet<>();
        Set<String> allowedSid = new HashSet<>();
        Map<String, Long> tQuantityById = new HashMap<>();
        Map<String, Long> allocatedByTid = new HashMap<>();
        for (T t : allowedT) {
            if (!pid.equals(t.pid())) {
                throw new IllegalStateException("allocate output has T with different pid");
            }
            if (!isAllocatedWithinTotal(t.q(), t.q_a())) {
                throw new IllegalStateException("allocate output has T q_a outside signed bounds of q");
            }
            allowedTid.add(t.id());
            tQuantityById.put(t.id(), t.q());
        }
        for (S s : allowedS) {
            if (!pid.equals(s.pid())) {
                throw new IllegalStateException("allocate output has S with different pid");
            }
            if (!isAllocatedWithinTotal(s.q() + s.q_carry(), s.q_a())) {
                throw new IllegalStateException("allocate output has S q_a outside signed bounds of q + q_carry");
            }
            allowedSid.add(s.id());
        }

        for (TS ts : emittedTs) {
            if (!pid.equals(ts.pid())) {
                throw new IllegalStateException("allocate output has TS with different pid");
            }
            if (!allowedTid.contains(ts.tid())) {
                throw new IllegalStateException("allocate output references unknown TS.tid " + ts.tid());
            }
            if (!allowedSid.contains(ts.sid())) {
                throw new IllegalStateException("allocate output references unknown TS.sid " + ts.sid());
            }
            if (ts.q_a() == 0) {
                throw new IllegalStateException("allocate output has TS.q_a == 0");
            }
            long allocatedForTid = allocatedByTid.getOrDefault(ts.tid(), 0L) + ts.q_a();
            allocatedByTid.put(ts.tid(), allocatedForTid);
            long sourceQ = tQuantityById.get(ts.tid());
            if (!isAllocatedWithinTotal(sourceQ, allocatedForTid)) {
                throw new IllegalStateException("allocate output over-allocates T id " + ts.tid());
            }
        }
    }

    private static void requireSamePid(List<String> pids, String expectedPid, String entityName) {
        for (String pid : pids) {
            if (!expectedPid.equals(pid)) {
                throw new IllegalArgumentException(entityName + " pid mismatch");
            }
        }
    }

    private static long remainingT(T t) {
        return signedRemaining(t.q(), t.q_a());
    }

    private static long remainingS(S s) {
        SignedSupplyUsage usage = supplyUsage(s);
        return usage.remainingCarry() + usage.remainingRegular();
    }

    private static long signedRemaining(long total, long allocated) {
        return total - allocated;
    }

    private static boolean areSignCompatible(long lhs, long rhs) {
        return lhs != 0 && rhs != 0 && Long.signum(lhs) == Long.signum(rhs);
    }

    private static long signedAllocation(long targetOpen, long sourceOpen) {
        if (!areSignCompatible(targetOpen, sourceOpen)) {
            return 0L;
        }
        long magnitude = Math.min(Math.abs(targetOpen), Math.abs(sourceOpen));
        return Long.signum(targetOpen) * magnitude;
    }

    private static boolean isAllocatedWithinTotal(long total, long allocated) {
        if (allocated == 0L) {
            return true;
        }
        if (total == 0L || Long.signum(total) != Long.signum(allocated)) {
            return false;
        }
        return Math.abs(allocated) <= Math.abs(total);
    }

    private static SignedSupplyUsage supplyUsage(S s) {
        long sign = Long.signum(s.q() + s.q_carry());
        long carryMagnitude = Math.abs(s.q_carry());
        long regularMagnitude = Math.abs(s.q());
        long usedMagnitude = Math.abs(s.q_a());

        long carryUsedMagnitude = Math.min(usedMagnitude, carryMagnitude);
        long regularUsedMagnitude = Math.min(Math.max(usedMagnitude - carryMagnitude, 0L), regularMagnitude);

        long remainingCarry = sign * (carryMagnitude - carryUsedMagnitude);
        long remainingRegular = sign * (regularMagnitude - regularUsedMagnitude);
        return new SignedSupplyUsage(remainingCarry, remainingRegular);
    }

    record AllocationResult(
            T updatedIncomingT,
            List<S> updatedS,
            S updatedIncomingS,
            List<T> updatedT,
            List<TS> emittedTs
    ) {
        AllocationResult {
            updatedS = List.copyOf(updatedS);
            updatedT = List.copyOf(updatedT);
            emittedTs = List.copyOf(emittedTs);
        }

        AllocationResult(T updatedIncomingT, List<S> updatedS, List<TS> emittedTs) {
            this(updatedIncomingT, updatedS, null, List.of(), emittedTs);
        }
    }

    private record SignedSupplyUsage(long remainingCarry, long remainingRegular) {
    }
}
