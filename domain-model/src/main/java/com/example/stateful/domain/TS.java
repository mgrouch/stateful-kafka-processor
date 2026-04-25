package com.example.stateful.domain;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.time.LocalDate;
import java.util.Objects;

public record TS(String id,
                 String pid,
                 String tid,
                 String sid,
                 LocalDate tDate,
                 LocalDate sDate,
                 long q,
                 long q_a_delta,
                 @JsonAlias("q_a") long q_a_total_after,
                 TT tt) {
    public TS(String id, String pid, String tid, String sid, long q, long q_a_delta) {
        this(id, pid, tid, sid, null, null, q, q_a_delta, q_a_delta, TT.B);
    }

    public TS(String id, String pid, String tid, String sid, LocalDate tDate, LocalDate sDate, long q, long q_a_delta) {
        this(id, pid, tid, sid, tDate, sDate, q, q_a_delta, q_a_delta, TT.B);
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
        if (Long.signum(q_a_delta) != Long.signum(q)) {
            throw new IllegalArgumentException("q_a_delta must have same sign as q");
        }
        if (Math.abs(q_a_delta) > Math.abs(q)) {
            throw new IllegalArgumentException("q_a_delta must satisfy abs(q_a_delta) <= abs(q)");
        }
        if (Long.signum(q_a_total_after) != 0 && Long.signum(q_a_total_after) != Long.signum(q)) {
            throw new IllegalArgumentException("q_a_total_after must have same sign as q");
        }
        if (Math.abs(q_a_total_after) > Math.abs(q)) {
            throw new IllegalArgumentException("q_a_total_after must satisfy abs(q_a_total_after) <= abs(q)");
        }
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
