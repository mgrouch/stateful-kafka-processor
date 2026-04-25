package com.example.stateful.processor.logic;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TransitionsModel {

    private final AllocationStrategy allocationStrategy;

    public TransitionsModel(AllocationStrategy allocationStrategy) {
        this.allocationStrategy = Objects.requireNonNull(allocationStrategy, "allocationStrategy must not be null");
    }

    public AllocationResult allocateForIncomingT(T incomingT, List<S> candidates, String idPrefix) {
        requireSamePid(candidates.stream().map(S::pid).toList(), incomingT.pid(), "S candidate");
        AllocationResult result = allocationStrategy.allocateForIncomingT(incomingT, candidates, List.of(), idPrefix);
        validateAllocationOutput(result.updatedIncomingT().pid(), List.of(result.updatedIncomingT()), result.updatedS(), result.emittedTs());
        return result;
    }

    public AllocationResult allocateForIncomingS(List<T> candidates, S incomingS, String idPrefix) {
        requireSamePid(candidates.stream().map(T::pid).toList(), incomingS.pid(), "T candidate");
        AllocationResult result = allocationStrategy.allocateForIncomingS(candidates, List.of(), incomingS, idPrefix);
        validateAllocationOutput(result.updatedIncomingS().pid(), result.updatedT(), List.of(result.updatedIncomingS()), result.emittedTs());
        return result;
    }

    public boolean isOpen(T t) {
        return remainingT(t) != 0;
    }

    public boolean isOpen(S s) {
        return remainingS(s) != 0;
    }

    public static void requireSamePid(List<String> pids, String expectedPid, String entityName) {
        for (String pid : pids) {
            if (!expectedPid.equals(pid)) {
                throw new IllegalArgumentException(entityName + " pid mismatch");
            }
        }
    }

    public static long remainingT(T t) {
        return signedRemaining(t.q(), t.q_a_total());
    }

    public static long remainingS(S s) {
        SignedSupplyUsage usage = SignedSupplyUsage.supplyUsage(s);
        return usage.remainingCarry() + usage.remainingRegular();
    }

    public static long signedRemaining(long total, long allocated) {
        return total - allocated;
    }

    public static boolean isAllocatedWithinTotal(long total, long allocated) {
        if (allocated == 0L) {
            return true;
        }
        if (total == 0L || Long.signum(total) != Long.signum(allocated)) {
            return false;
        }
        return Math.abs(allocated) <= Math.abs(total);
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
            if (!isAllocatedWithinTotal(t.q(), t.q_a_total())) {
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
            if (ts.q_a_delta() == 0) {
                throw new IllegalStateException("allocate output has TS.q_a_delta == 0");
            }
            long allocatedForTid = allocatedByTid.getOrDefault(ts.tid(), 0L) + ts.q_a_delta();
            allocatedByTid.put(ts.tid(), allocatedForTid);
            long sourceQ = tQuantityById.get(ts.tid());
            if (!isAllocatedWithinTotal(sourceQ, allocatedForTid)) {
                throw new IllegalStateException("allocate output over-allocates T id " + ts.tid());
            }
            if (!isAllocatedWithinTotal(sourceQ, ts.q_a_total_after())) {
                throw new IllegalStateException("allocate output has TS.q_a_total_after outside signed bounds of q");
            }
        }
    }
}
