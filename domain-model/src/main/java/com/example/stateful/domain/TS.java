package com.example.stateful.domain;

import java.util.Objects;
import java.time.LocalDate;

public record TS(String id, String pid, String tid, String sid, LocalDate tDate, LocalDate sDate, long q, long q_a, TT tt) {
    public TS(String id, String pid, String tid, String sid, long q, long q_a) {
        this(id, pid, tid, sid, null, null, q, q_a, TT.B);
    }

    public TS(String id, String pid, String tid, String sid, LocalDate tDate, LocalDate sDate, long q, long q_a) {
        this(id, pid, tid, sid, tDate, sDate, q, q_a, TT.B);
    }

    public TS {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(tid, "tid");
        requireText(sid, "sid");
        tt = tt == null ? TT.B : tt;
        if (q == 0) {
            throw new IllegalArgumentException("q must not be 0");
        }
        if (Long.signum(q_a) != Long.signum(q)) {
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
