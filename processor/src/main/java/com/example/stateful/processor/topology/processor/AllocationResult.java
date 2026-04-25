package com.example.stateful.processor.topology.processor;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.List;

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
