package com.example.stateful.domain;

import java.util.Objects;

public record S(String id, String pid, long q, long q_carry, long q_a, boolean rollover) {
    public S(String id, String pid, long q, long q_a) {
        this(id, pid, q, 0L, q_a, false);
    }

    public S(String id, String pid, long q, long q_a, boolean rollover) {
        this(id, pid, q, 0L, q_a, rollover);
    }

    public S {
        requireText(id, "id");
        requireText(pid, "pid");

        long total = q + q_carry;
        if (total == 0L) {
            throw new IllegalArgumentException("q + q_carry must not be 0");
        }
        if (Long.signum(q) != 0 && Long.signum(q_carry) != 0 && Long.signum(q) != Long.signum(q_carry)) {
            throw new IllegalArgumentException("q and q_carry must have the same sign when both non-zero");
        }
        if (Long.signum(q_a) != 0 && Long.signum(q_a) != Long.signum(total)) {
            throw new IllegalArgumentException("q_a must have same sign as q + q_carry");
        }
        if (Math.abs(q_a) > Math.abs(total)) {
            throw new IllegalArgumentException("q_a must satisfy abs(q_a) <= abs(q + q_carry)");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
