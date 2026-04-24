package com.example.stateful.domain;

import java.util.Objects;

public record T(String id, String pid, long q) {
    public T {
        requireText(id, "id");
        requireText(pid, "pid");
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
