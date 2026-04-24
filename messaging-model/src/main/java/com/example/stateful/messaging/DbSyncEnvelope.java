package com.example.stateful.messaging;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.Objects;

public record DbSyncEnvelope(
        String eventId,
        DbSyncMutationType mutationType,
        String pid,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        long sourceTimestamp,
        int eventOrdinal,
        T t,
        S s,
        TS ts
) {

    public DbSyncEnvelope {
        requireText(eventId, "eventId");
        Objects.requireNonNull(mutationType, "mutationType must not be null");
        requireText(pid, "pid");
        requireText(sourceTopic, "sourceTopic");
        if (sourcePartition < 0) {
            throw new IllegalArgumentException("sourcePartition must be >= 0");
        }
        if (sourceOffset < 0) {
            throw new IllegalArgumentException("sourceOffset must be >= 0");
        }
        if (eventOrdinal < 1) {
            throw new IllegalArgumentException("eventOrdinal must be >= 1");
        }
        validatePayloads(mutationType, t, s, ts);
    }

    private static void validatePayloads(DbSyncMutationType mutationType, T t, S s, TS ts) {
        switch (mutationType) {
            case ACCEPTED_T, UPSERT_UNPROCESSED_T -> {
                if (t == null || s != null || ts != null) {
                    throw new IllegalArgumentException(mutationType + " requires only t payload");
                }
            }
            case ACCEPTED_S, UPSERT_UNPROCESSED_S -> {
                if (s == null || t != null || ts != null) {
                    throw new IllegalArgumentException(mutationType + " requires only s payload");
                }
            }
            case GENERATED_TS -> {
                if (ts == null || t != null || s != null) {
                    throw new IllegalArgumentException("GENERATED_TS requires only ts payload");
                }
            }
            case DELETE_UNPROCESSED_T, DELETE_UNPROCESSED_S -> {
                if (t != null || s != null || ts != null) {
                    throw new IllegalArgumentException(mutationType + " does not support payload");
                }
            }
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
