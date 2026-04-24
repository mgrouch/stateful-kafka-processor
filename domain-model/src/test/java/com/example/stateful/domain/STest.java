package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class STest {

    @Test
    void qSignMustMatchDir() {
        assertDoesNotThrow(() -> new S("s-d", "IBM", null, -10L, 0L, 0L, false, Dir.D));
        assertDoesNotThrow(() -> new S("s-r", "IBM", null, 10L, 0L, 0L, false, Dir.R));

        assertThrows(IllegalArgumentException.class,
                () -> new S("s-bad-d", "IBM", null, 10L, 0L, 0L, false, Dir.D));
        assertThrows(IllegalArgumentException.class,
                () -> new S("s-bad-r", "IBM", null, -10L, 0L, 0L, false, Dir.R));
    }

    @Test
    void ctorWithoutDirInfersFromQSign() {
        assertDoesNotThrow(() -> new S("s-neg", "IBM", -10L, 0L));
        assertDoesNotThrow(() -> new S("s-pos", "IBM", 10L, 0L));
    }
}
