package com.example.stateful.domain;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.time.LocalDate;
import java.util.Objects;

public record T(String id, String pid, String ref, String accId,
                TT tt, LocalDate tDate, LocalDate sDate, AStatus a_status, boolean cancel,
                long q, @JsonAlias("q_a") long q_a_total, long q_a_delta_last, long q_f
) {
    public T(String id, String pid, String ref, boolean cancel, long q, long q_a_total) {
        this(id, pid, ref, null, TT.B, null, null, AStatus.NORM, cancel, q, q_a_total, 0L, 0L);
    }

    public T(String id, String pid, String ref, boolean cancel, long q, long q_a_total, AStatus a_status) {
        this(id, pid, ref, null, TT.B, null, null, a_status, cancel, q, q_a_total, 0L, 0L);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total) {
        this(id, pid, ref, null, TT.B, tDate, null, AStatus.NORM, cancel, q, q_a_total, 0L, 0L);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total, AStatus a_status) {
        this(id, pid, ref, null, TT.B, tDate, null, a_status, cancel, q, q_a_total, 0L, 0L);
    }

    public T(String id, String pid, String ref, LocalDate tDate, boolean cancel, long q, long q_a_total, AStatus a_status, TT tt) {
        this(id, pid, ref, null, tt, tDate, null, a_status, cancel, q, q_a_total, 0L, 0L);
    }

    public T(String id, String pid, String ref, LocalDate tDate, LocalDate sDate, boolean cancel, long q, long q_a_total, long q_a_delta_last, AStatus a_status, TT tt) {
        this(id, pid, ref, null, tt, tDate, sDate, a_status, cancel, q, q_a_total, q_a_delta_last, 0L);
    }

    public T(String id, String pid, String ref, LocalDate tDate, LocalDate sDate, boolean cancel, long q, long q_a_total, long q_a_delta_last, long q_f, AStatus a_status, TT tt) {
        this(id, pid, ref, null, tt, tDate, sDate, a_status, cancel, q, q_a_total, q_a_delta_last, q_f);
    }

    public T {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(ref, "ref");
        a_status = a_status == null ? AStatus.NORM : a_status;
        if (accId != null && accId.isBlank()) {
            throw new IllegalArgumentException("accId must not be blank when provided");
        }
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
        if (a_status == AStatus.FAIL && q_f == 0L) {
            throw new IllegalArgumentException("q_f must not be 0 when a_status is FAIL");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
