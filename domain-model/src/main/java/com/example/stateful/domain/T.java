package com.example.stateful.domain;

import java.util.Objects;

public record T(String id, String pid, String ref, boolean cancel, long q, long q_a, AllocationStatus a_status) {
    public T(String id, String pid, String ref, boolean cancel, long q, long q_a) {
        this(id, pid, ref, cancel, q, q_a, AllocationStatus.NORMAL);
    }

    public T {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(ref, "ref");
        a_status = a_status == null ? AllocationStatus.NORMAL : a_status;
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
