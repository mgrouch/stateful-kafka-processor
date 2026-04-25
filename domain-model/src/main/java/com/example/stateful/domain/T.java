package com.example.stateful.domain;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.time.LocalDate;
import java.util.Objects;

public record T(String id,
                String pid,
                String ref,
                LocalDate tDate,
                boolean cancel,
                long q,
                @JsonAlias("q_a") long q_a_total,
                long q_a_delta_last,
                AStatus a_status,
                TT tt) {
    public T(String id, String pid, String ref, boolean cancel, long q, long q_a_total) {
        this(id, pid, ref, null, cancel, q, q_a_total, 0L, AStatus.NORMAL, TT.B);
    }

    public T(String id, String pid, String ref, boolean cancel, long q, long q_a_total, AStatus a_status) {
        this(id, pid, ref, null, cancel, q, q_a_total, 0L, a_status, TT.B);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total) {
        this(id, pid, ref, tDate, cancel, q, q_a_total, 0L, AStatus.NORMAL, TT.B);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total, AStatus a_status) {
        this(id, pid, ref, tDate, cancel, q, q_a_total, 0L, a_status, TT.B);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total, AStatus a_status, TT tt) {
        this(id, pid, ref, tDate, cancel, q, q_a_total, 0L, a_status, tt);
    }

    public T {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(ref, "ref");
        a_status = a_status == null ? AStatus.NORMAL : a_status;
        tt = tt == null ? TT.B : tt;

        if (q == 0L) {
            throw new IllegalArgumentException("q must not be 0");
        }
        switch (tt) {
            case B, CS -> {
                if (q <= 0L) {
                    throw new IllegalArgumentException("q must be > 0 for tt B/CS");
                }
            }
            case S, SS -> {
                if (q >= 0L) {
                    throw new IllegalArgumentException("q must be < 0 for tt S/SS");
                }
            }
        }
        if (Long.signum(q_a_total) != 0 && Long.signum(q_a_total) != Long.signum(q)) {
            throw new IllegalArgumentException("q_a_total must have same sign as q");
        }
        if (Math.abs(q_a_total) > Math.abs(q)) {
            throw new IllegalArgumentException("q_a_total must satisfy abs(q_a_total) <= abs(q)");
        }
        if (q_a_delta_last != 0L && Long.signum(q_a_delta_last) != Long.signum(q)) {
            throw new IllegalArgumentException("q_a_delta_last must have same sign as q");
        }
        if (Math.abs(q_a_delta_last) > Math.abs(q)) {
            throw new IllegalArgumentException("q_a_delta_last must satisfy abs(q_a_delta_last) <= abs(q)");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
