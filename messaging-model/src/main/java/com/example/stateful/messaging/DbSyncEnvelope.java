package com.example.stateful.messaging;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.Objects;

public record DbSyncEnvelope(
        DbSyncMutationType mutationType,
        String pid,
        T t,
        S s,
        TS ts,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        long sourceTimestamp,
        int ordinal,
        String eventId
) {

    public DbSyncEnvelope {
        Objects.requireNonNull(mutationType, "mutationType must not be null");
        requireText(pid, "pid");
        requireText(sourceTopic, "sourceTopic");
        if (sourcePartition < 0) {
            throw new IllegalArgumentException("sourcePartition must be >= 0");
        }
        if (sourceOffset < 0) {
            throw new IllegalArgumentException("sourceOffset must be >= 0");
        }
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must be >= 0");
        }
        requireText(eventId, "eventId");
        validatePayload(mutationType, t, s, ts);
    }

    private static void validatePayload(DbSyncMutationType type, T t, S s, TS ts) {
        switch (type) {
            case ACCEPTED_T, UPSERT_UNPROCESSED_T -> {
                if (t == null || s != null || ts != null) {
                    throw new IllegalArgumentException(type + " requires only t payload");
                }
            }
            case ACCEPTED_S, UPSERT_UNPROCESSED_S -> {
                if (s == null || t != null || ts != null) {
                    throw new IllegalArgumentException(type + " requires only s payload");
                }
            }
            case GENERATED_TS -> {
                if (ts == null || t != null || s != null) {
                    throw new IllegalArgumentException("GENERATED_TS requires only ts payload");
                }
            }
            case DELETE_UNPROCESSED_T, DELETE_UNPROCESSED_S -> {
                if (t != null || s != null || ts != null) {
                    throw new IllegalArgumentException(type + " does not include payload");
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
