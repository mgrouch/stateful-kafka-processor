package com.example.stateful.domain;

import java.util.Objects;

public record TS(String id, String pid, String tid, String sid, long q_a) {
    public TS {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(tid, "tid");
        requireText(sid, "sid");
        if (q_a <= 0) {
            throw new IllegalArgumentException("q_a must be > 0");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
