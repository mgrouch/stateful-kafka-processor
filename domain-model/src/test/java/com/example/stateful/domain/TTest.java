package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TTest {

    @Test
    void buyTypesRequirePositiveQ() {
        assertDoesNotThrow(() -> new T("t-1", "IBM", "R-1", null, false, 10L, 0L, AStatus.NORMAL, TT.B));
        assertDoesNotThrow(() -> new T("t-2", "IBM", "R-2", null, false, 10L, 0L, AStatus.NORMAL, TT.CS));

        assertThrows(IllegalArgumentException.class,
                () -> new T("t-3", "IBM", "R-3", null, false, -10L, 0L, AStatus.NORMAL, TT.B));
        assertThrows(IllegalArgumentException.class,
                () -> new T("t-4", "IBM", "R-4", null, false, -10L, 0L, AStatus.NORMAL, TT.CS));
    }

    @Test
    void sellTypesRequireNegativeQ() {
        assertDoesNotThrow(() -> new T("t-5", "IBM", "R-5", null, false, -10L, 0L, AStatus.NORMAL, TT.S));
        assertDoesNotThrow(() -> new T("t-6", "IBM", "R-6", null, false, -10L, 0L, AStatus.NORMAL, TT.SS));

        assertThrows(IllegalArgumentException.class,
                () -> new T("t-7", "IBM", "R-7", null, false, 10L, 0L, AStatus.NORMAL, TT.S));
        assertThrows(IllegalArgumentException.class,
                () -> new T("t-8", "IBM", "R-8", null, false, 10L, 0L, AStatus.NORMAL, TT.SS));
    }
}
