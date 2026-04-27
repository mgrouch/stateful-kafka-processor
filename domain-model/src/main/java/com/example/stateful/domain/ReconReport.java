package com.example.stateful.domain;

import java.util.Objects;

public record ReconReport(
        String id,
        String pid,
        String sid,
        long q_tot,
        long q_a_tot,
        long q_f_tot,
        long q_s_tot,
        long q_s_carry
) {
    public ReconReport {
        requireText(id, "id");
        requireText(pid, "pid");
        requireText(sid, "sid");
    }

    private static void requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
