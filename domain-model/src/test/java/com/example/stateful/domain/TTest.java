package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TTest {

    @Test
    void buyTypesRequirePositiveQ() {
        assertDoesNotThrow(() -> new T("t-1", "AAA", "R-1", null, false, 10L, 0L, AStatus.NORM, TT.B));
        assertDoesNotThrow(() -> new T("t-2", "AAA", "R-2", null, false, 10L, 0L, AStatus.NORM, TT.CS));

        assertThrows(IllegalArgumentException.class,
                () -> new T("t-3", "AAA", "R-3", null, false, -10L, 0L, AStatus.NORM, TT.B));
        assertThrows(IllegalArgumentException.class,
                () -> new T("t-4", "AAA", "R-4", null, false, -10L, 0L, AStatus.NORM, TT.CS));
    }

    @Test
    void sellTypesRequireNegativeQ() {
        assertDoesNotThrow(() -> new T("t-5", "AAA", "R-5", null, false, -10L, 0L, AStatus.NORM, TT.S));
        assertDoesNotThrow(() -> new T("t-6", "AAA", "R-6", null, false, -10L, 0L, AStatus.NORM, TT.SS));

        assertThrows(IllegalArgumentException.class,
                () -> new T("t-7", "AAA", "R-7", null, false, 10L, 0L, AStatus.NORM, TT.S));
        assertThrows(IllegalArgumentException.class,
                () -> new T("t-8", "AAA", "R-8", null, false, 10L, 0L, AStatus.NORM, TT.SS));
    }
}
