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

        if (q == 0L) {
            throw new IllegalArgumentException("q must not be 0");
        }
        if (Long.signum(q_a) != 0 && Long.signum(q_a) != Long.signum(q)) {
            throw new IllegalArgumentException("q_a must have same sign as q");
        }
        if (Math.abs(q_a) > Math.abs(q)) {
            throw new IllegalArgumentException("q_a must satisfy abs(q_a) <= abs(q)");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
