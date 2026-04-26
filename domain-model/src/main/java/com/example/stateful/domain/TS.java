package com.example.stateful.domain;

import java.time.LocalDate;
import java.util.Objects;

public record TS(String id,
                 String pid,
                 String pidAlt1,
                 String pidAlt2,
                 String tid,
                 String sid,
                 String accId,
                 LocalDate tDate,
                 LocalDate sDate,
                 long q,
                 long q_a_delta,
                 long q_a_total_after,
                 TT tt,
                 ActType activity,
                 MStatus mStatus,
                 boolean o,
                 boolean cancel) {
    public TS(String id, String pid, String tid, String sid, long q, long q_a_delta) {
        this(id, pid, null, null, tid, sid, null, null, null, q, q_a_delta, q_a_delta, TT.B, ActType.A01, MStatus.U, false, false);
    }

    public TS(String id, String pid, String tid, String sid, LocalDate tDate, LocalDate sDate, long q, long q_a_delta) {
        this(id, pid, null, null, tid, sid, null, tDate, sDate, q, q_a_delta, q_a_delta, TT.B, ActType.A01, MStatus.U, false, false);
    }

    public TS(String id,
              String pid,
              String tid,
              String sid,
              String accId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt) {
        this(id, pid, null, null, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, false, false);
    }

    public TS(String id,
              String pid,
              String pidAlt1,
              String pidAlt2,
              String tid,
              String sid,
              String accId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt) {
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, false, false);
    }

    public TS(String id,
              String pid,
              String pidAlt1,
              String pidAlt2,
              String tid,
              String sid,
              String accId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt,
              boolean o) {
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, false);
    }

    public TS(String id,
              String pid,
              String tid,
              String sid,
              String accId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt,
              boolean o) {
        this(id, pid, null, null, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, false);
    }

    public TS(String id,
              String pid,
              String pidAlt1,
              String pidAlt2,
              String tid,
              String sid,
              String accId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt,
              boolean o,
              boolean cancel) {
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, cancel);
    }

    public TS {
        requireText(id, "id");
        requireText(pid, "pid");
        if (pidAlt1 != null && pidAlt1.isBlank()) {
            throw new IllegalArgumentException("pidAlt1 must not be blank when provided");
        }
        if (pidAlt2 != null && pidAlt2.isBlank()) {
            throw new IllegalArgumentException("pidAlt2 must not be blank when provided");
        }
        requireText(tid, "tid");
        requireText(sid, "sid");
        if (accId != null && accId.isBlank()) {
            throw new IllegalArgumentException("accId must not be blank when provided");
        }
        tt = tt == null ? TT.B : tt;
        activity = activity == null ? ActType.A01 : activity;
        mStatus = mStatus == null ? MStatus.U : mStatus;
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
