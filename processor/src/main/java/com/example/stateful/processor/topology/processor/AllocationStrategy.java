package com.example.stateful.processor.topology.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;

import java.util.List;

public interface AllocationStrategy {

    long allocate(long targetOpen, long sourceOpen);

    default boolean areSignCompatible(long lhs, long rhs) {
        return lhs != 0 && rhs != 0 && Long.signum(lhs) == Long.signum(rhs);
    }

    default List<S> orderSCandidatesForIncomingT(List<S> candidates, T incomingT) {
        return candidates;
    }

    default List<T> orderTCandidatesForIncomingS(List<T> candidates, S incomingS) {
        return candidates;
    }
}
