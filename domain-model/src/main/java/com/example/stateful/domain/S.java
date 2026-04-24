package com.example.stateful.domain;

import java.util.Objects;

public record S(String id, String pid, long q, long q_a) {
    public S {
        requireText(id, "id");
        requireText(pid, "pid");
        if (q < 0) {
            throw new IllegalArgumentException("q must be >= 0");
        }
        if (q_a < 0 || q_a > q) {
            throw new IllegalArgumentException("q_a must satisfy 0 <= q_a <= q");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
