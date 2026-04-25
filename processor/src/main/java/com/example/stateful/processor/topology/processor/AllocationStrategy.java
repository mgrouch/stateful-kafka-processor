package com.example.stateful.processor.topology.processor;

public class AllocationStrategy {

    public long allocate(long targetOpen, long sourceOpen) {
        if (!areSignCompatible(targetOpen, sourceOpen)) {
            return 0L;
        }
        long magnitude = Math.min(Math.abs(targetOpen), Math.abs(sourceOpen));
        return Long.signum(targetOpen) * magnitude;
    }

    private static boolean areSignCompatible(long lhs, long rhs) {
        return lhs != 0 && rhs != 0 && Long.signum(lhs) == Long.signum(rhs);
    }
}
