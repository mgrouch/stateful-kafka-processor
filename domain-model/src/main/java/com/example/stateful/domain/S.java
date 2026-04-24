package com.example.stateful.domain;

import java.util.Objects;
import java.time.LocalDate;

public record S(String id, String pid, LocalDate bDate, long q, long q_carry, long q_a, boolean rollover, Dir dir) {
    public S(String id, String pid, long q, long q_a) {
        this(id, pid, null, q, 0L, q_a, false, inferDir(q));
    }

    public S(String id, String pid, long q, long q_a, boolean rollover) {
        this(id, pid, null, q, 0L, q_a, rollover, inferDir(q));
    }

    public S(String id, String pid, long q, long q_carry, long q_a, boolean rollover) {
        this(id, pid, null, q, q_carry, q_a, rollover, inferDir(q));
    }

    public S(String id, String pid, LocalDate bDate, long q, long q_a) {
        this(id, pid, bDate, q, 0L, q_a, false, inferDir(q));
    }

    public S(String id, String pid, LocalDate bDate, long q, long q_a, boolean rollover) {
        this(id, pid, bDate, q, 0L, q_a, rollover, inferDir(q));
    }

    public S(String id, String pid, LocalDate bDate, long q, long q_carry, long q_a, boolean rollover) {
        this(id, pid, bDate, q, q_carry, q_a, rollover, inferDir(q));
    }

    public S {
        requireText(id, "id");
        requireText(pid, "pid");
        dir = dir == null ? inferDir(q) : dir;

        long total = q + q_carry;
        if (total == 0L) {
            throw new IllegalArgumentException("q + q_carry must not be 0");
        }
        if (Long.signum(q) != 0 && Long.signum(q_carry) != 0 && Long.signum(q) != Long.signum(q_carry)) {
            throw new IllegalArgumentException("q and q_carry must have the same sign when both non-zero");
        }
        if (dir == Dir.D && q >= 0L) {
            throw new IllegalArgumentException("q must be < 0 when dir is D");
        }
        if (dir == Dir.R && q <= 0L) {
            throw new IllegalArgumentException("q must be > 0 when dir is R");
        }
        if (Long.signum(q_a) != 0 && Long.signum(q_a) != Long.signum(total)) {
            throw new IllegalArgumentException("q_a must have same sign as q + q_carry");
        }
        if (Math.abs(q_a) > Math.abs(total)) {
            throw new IllegalArgumentException("q_a must satisfy abs(q_a) <= abs(q + q_carry)");
        }
    }

    private static Dir inferDir(long q) {
        return q < 0L ? Dir.D : Dir.R;
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
