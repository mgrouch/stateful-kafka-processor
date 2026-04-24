package com.example.stateful.processor;

public final class PidPartitioner {

    private PidPartitioner() {
    }

    public static int partitionFor(String pid, int totalPartitions) {
        return Math.floorMod(pid.hashCode(), totalPartitions);
    }
}
