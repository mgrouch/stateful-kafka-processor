package com.example.stateful.processor;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.utils.Utils;

public final class PidPartitioner {
    private PidPartitioner() {
    }

    public static int partitionFor(String pid, int totalPartitions) {
        if (pid == null) {
            throw new IllegalArgumentException("pid must not be null");
        }
        if (totalPartitions <= 0) {
            throw new IllegalArgumentException("totalPartitions must be > 0");
        }

        byte[] serializedKey = pid.getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(serializedKey)) % totalPartitions;
    }
}
