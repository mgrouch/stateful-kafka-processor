package com.example.stateful.processor.logic;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public final class NaiveAlocationStrategy implements AllocationStrategy {

    private static final String LOTTERY_INCOMING_S = "INCOMING_S";
    private static final String LOTTERY_FAIL_BUCKET = "FAIL";
    private static final String LOTTERY_NORM_BUCKET = "NORM";

    private final long allocationLotterySeed;

    public NaiveAlocationStrategy() {
        this(1357911L);
    }

    public NaiveAlocationStrategy(long allocationLotterySeed) {
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
        List<T> fail = canonical.stream().filter(t -> t.a_status() == AStatus.FAIL).toList();
        List<T> normal = canonical.stream().filter(t -> t.a_status() != AStatus.FAIL).toList();

        List<T> ordered = new ArrayList<>(candidates.size());
        ordered.addAll(shuffleDeterministically(fail, incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_FAIL_BUCKET));
        ordered.addAll(shuffleDeterministically(normal, incomingS.pid(), incomingS.id(), LOTTERY_INCOMING_S, LOTTERY_NORM_BUCKET));
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
}
