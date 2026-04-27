package com.example.stateful.domain;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public record TS(String id,
                 String pid,
                 String pidAlt1,
                 String pidAlt2,
                 String tid,
                 String sid,
                 String accId,
                 String sorId,
                 String oarId,
                 LocalDate tDate,
                 LocalDate sDate,
                 long q,
                 long q_a_delta,
                 long q_a_total_after,
                 TT tt,
                 ActType activity,
                 MStatus mStatus,
                 boolean o,
                 boolean cancel,
                 List<S> extraS,
                 String refTS,
                 TCycle tCycle,
                 SMode sMode,
                 AStatus a_status,
                 long q_f,
                 Long ledgerTime) {
    public TS(String id,
              String pid,
              String pidAlt1,
              String pidAlt2,
              String tid,
              String sid,
              String accId,
              String sorId,
              String oarId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt,
              ActType activity,
              MStatus mStatus,
              boolean o,
              boolean cancel,
              List<S> extraS,
              String refTS) {
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, sorId, oarId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, activity, mStatus, o, cancel, extraS, refTS, TCycle.SD, SMode.CN, AStatus.NORM, 0L, null);
    }

    public TS(String id, String pid, String tid, String sid, long q, long q_a_delta) {
        this(id, pid, null, null, tid, sid, null, null, null, null, null, q, q_a_delta, q_a_delta, TT.B, ActType.A01, MStatus.U, false, false, null, null);
    }

    public TS(String id, String pid, String tid, String sid, LocalDate tDate, LocalDate sDate, long q, long q_a_delta) {
        this(id, pid, null, null, tid, sid, null, null, null, tDate, sDate, q, q_a_delta, q_a_delta, TT.B, ActType.A01, MStatus.U, false, false, null, null);
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
        this(id, pid, null, null, tid, sid, accId, null, null, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, false, false, null, null);
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
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, null, null, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, false, false, null, null);
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
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, null, null, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, false, null, null);
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
        this(id, pid, null, null, tid, sid, accId, null, null, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, false, null, null);
    }

    public TS(String id,
              String pid,
              String pidAlt1,
              String pidAlt2,
              String tid,
              String sid,
              String accId,
              String sorId,
              String oarId,
              LocalDate tDate,
              LocalDate sDate,
              long q,
              long q_a_delta,
              long q_a_total_after,
              TT tt,
              boolean o) {
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, sorId, oarId, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, false, null, null);
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
        this(id, pid, pidAlt1, pidAlt2, tid, sid, accId, null, null, tDate, sDate, q, q_a_delta, q_a_total_after, tt, ActType.A01, MStatus.U, o, cancel, null, null);
    }

    public TS {
        requireText(id, "id");
        requireText(pid, "pid");
        refTS = refTS == null ? id : refTS;
        requireText(refTS, "refTS");
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
        if (sorId != null && sorId.isBlank()) {
            throw new IllegalArgumentException("sorId must not be blank when provided");
        }
        if (oarId != null && oarId.isBlank()) {
            throw new IllegalArgumentException("oarId must not be blank when provided");
        }
        tt = tt == null ? TT.B : tt;
        activity = activity == null ? ActType.A01 : activity;
        mStatus = mStatus == null ? MStatus.U : mStatus;
        tCycle = tCycle == null ? TCycle.SD : tCycle;
        sMode = sMode == null ? SMode.CN : sMode;
        a_status = a_status == null ? AStatus.NORM : a_status;
        if (extraS != null) {
            if (extraS.size() > 3) {
                throw new IllegalArgumentException("extraS must contain at most 3 items");
            }
            if (extraS.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("extraS must not contain null items");
            }
            extraS = List.copyOf(extraS);
        }
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
