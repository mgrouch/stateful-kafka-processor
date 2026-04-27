package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReconReportTest {

    @Test
    void acceptsValidPayload() {
        assertDoesNotThrow(() -> new ReconReport("rr-1", "PID", "s-1", 10L, 2L, 1L, 5L, 3L));
    }

    @Test
    void rejectsBlankRequiredFields() {
        assertThrows(IllegalArgumentException.class, () -> new ReconReport(" ", "PID", "s-1", 0L, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReconReport("rr", " ", "s-1", 0L, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReconReport("rr", "PID", " ", 0L, 0L, 0L, 0L, 0L));
    }
}
