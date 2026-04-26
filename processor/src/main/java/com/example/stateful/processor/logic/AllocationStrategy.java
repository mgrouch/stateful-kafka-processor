package com.example.stateful.processor.logic;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public interface AllocationStrategy {

    long allocate(long targetOpen, long sourceOpen);

    default List<S> orderSCandidatesForIncomingT(List<S> candidates, T incomingT) {
        return candidates;
    }

    default List<T> orderTCandidatesForIncomingS(List<T> candidates, S incomingS) {
        return candidates;
    }

    default AllocationResult allocateForIncomingT(T incomingT, List<S> orderedCandidates, List<S> untouchedCandidates, String idPrefix) {
        List<S> updatedS = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        T updatedT = incomingT;
        int tsIndex = 0;

        for (S candidate : orderedCandidates) {
            long tRemaining = remainingT(updatedT);
            long sRemaining = remainingS(candidate);
            long allocated = allocate(tRemaining, sRemaining);

            if (allocated != 0) {
                long nextTotal = updatedT.q_a_total() + allocated;
                LocalDate allocatedSDate = nextTotal == updatedT.q() ? nextSDate(updatedT.sDate(), candidate.bDate()) : updatedT.sDate();
                updatedT = new T(updatedT.id(), updatedT.pid(), updatedT.ref(), updatedT.accId(), updatedT.tt(), updatedT.tDate(), allocatedSDate, updatedT.a_status(), updatedT.cancel(), updatedT.q(), nextTotal, allocated, updatedT.q_f(), updatedT.ledgerTime());
                S nextS = new S(candidate.id(), candidate.pid(), candidate.bDate(), candidate.q(), candidate.q_carry(), candidate.q_a() + allocated, candidate.q_a_opposite_delta(), candidate.q_a_opposite_total(), candidate.rollover(), candidate.o(), candidate.dir(), candidate.ledgerTime());
                updatedS.add(nextS);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedT.pid(), updatedT.id(), nextS.id(), updatedT.accId(), updatedT.tDate(), nextS.bDate(), updatedT.q(), allocated, nextTotal, updatedT.tt(), nextS.o()));
            } else {
                updatedS.add(candidate);
            }
        }
        updatedS.addAll(untouchedCandidates);
        return new AllocationResult(updatedT, updatedS, emitted);
    }

    default AllocationResult allocateForIncomingS(List<T> orderedCandidates, List<T> untouchedCandidates, S incomingS, String idPrefix) {
        List<T> updatedT = new ArrayList<>();
        List<TS> emitted = new ArrayList<>();
        S updatedS = incomingS;
        int tsIndex = 0;

        for (T candidate : orderedCandidates) {
            long sRemaining = remainingS(updatedS);
            long tRemaining = remainingT(candidate);
            long allocated = allocate(sRemaining, tRemaining);

            if (allocated != 0) {
                long nextTotal = candidate.q_a_total() + allocated;
                LocalDate allocatedSDate = nextTotal == candidate.q() ? nextSDate(candidate.sDate(), updatedS.bDate()) : candidate.sDate();
                T nextT = new T(candidate.id(), candidate.pid(), candidate.ref(), candidate.accId(), candidate.tt(), candidate.tDate(), allocatedSDate, candidate.a_status(), candidate.cancel(), candidate.q(), nextTotal, allocated, candidate.q_f(), candidate.ledgerTime());
                updatedS = new S(updatedS.id(), updatedS.pid(), updatedS.bDate(), updatedS.q(), updatedS.q_carry(), updatedS.q_a() + allocated, updatedS.q_a_opposite_delta(), updatedS.q_a_opposite_total(), updatedS.rollover(), updatedS.o(), updatedS.dir(), updatedS.ledgerTime());
                updatedT.add(nextT);
                emitted.add(new TS(idPrefix + "-" + (++tsIndex), updatedS.pid(), nextT.id(), updatedS.id(), nextT.accId(), nextT.tDate(), updatedS.bDate(), nextT.q(), allocated, nextTotal, nextT.tt(), updatedS.o()));
            } else {
                updatedT.add(candidate);
            }
        }
        updatedT.addAll(untouchedCandidates);
        return new AllocationResult(null, List.of(), updatedS, updatedT, emitted);
    }

    private static long remainingT(T t) {
        return t.q() - t.q_a_total();
    }

    private static long remainingS(S s) {
        SignedSupplyUsage usage = SignedSupplyUsage.supplyUsage(s);
        return usage.remainingCarry() + usage.remainingRegular();
    }

    private static LocalDate nextSDate(LocalDate currentSDate, LocalDate allocationSDate) {
        return allocationSDate != null ? allocationSDate : currentSDate;
    }
}
