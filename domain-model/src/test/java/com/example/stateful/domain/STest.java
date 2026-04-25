package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class STest {

    @Test
    void qSignMustMatchDir() {
        assertDoesNotThrow(() -> new S("s-d", "AAA", null, -10L, 0L, 0L, false, Dir.D));
        assertDoesNotThrow(() -> new S("s-r", "AAA", null, 10L, 0L, 0L, false, Dir.R));

        assertThrows(IllegalArgumentException.class,
                () -> new S("s-bad-d", "AAA", null, 10L, 0L, 0L, false, Dir.D));
        assertThrows(IllegalArgumentException.class,
                () -> new S("s-bad-r", "AAA", null, -10L, 0L, 0L, false, Dir.R));
    }

    @Test
    void ctorWithoutDirInfersFromQSign() {
        assertDoesNotThrow(() -> new S("s-neg", "AAA", -10L, 0L));
        assertDoesNotThrow(() -> new S("s-pos", "AAA", 10L, 0L));
    }
}
