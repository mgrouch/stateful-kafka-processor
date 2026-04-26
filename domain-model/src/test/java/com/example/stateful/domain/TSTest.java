package com.example.stateful.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TSTest {

    @Test
    void extraSAllowsUpToThreeItems() {
        List<S> extraS = List.of(
                new S("s-1", "AAA", 10L, 0L),
                new S("s-2", "AAA", 11L, 0L),
                new S("s-3", "AAA", 12L, 0L)
        );

        assertDoesNotThrow(() -> new TS(
                "ts-1", "AAA", null, null, "t-1", "s-1", null, null, null,
                null, null, 100L, 10L, 10L, TT.B, ActType.A01, MStatus.U, false, false, extraS, null
        ));
    }

    @Test
    void extraSRejectsMoreThanThreeItems() {
        List<S> extraS = List.of(
                new S("s-1", "AAA", 10L, 0L),
                new S("s-2", "AAA", 11L, 0L),
                new S("s-3", "AAA", 12L, 0L),
                new S("s-4", "AAA", 13L, 0L)
        );

        assertThrows(IllegalArgumentException.class, () -> new TS(
                "ts-2", "AAA", null, null, "t-2", "s-2", null, null, null,
                null, null, 100L, 10L, 10L, TT.B, ActType.A01, MStatus.U, false, false, extraS, null
        ));
    }

    @Test
    void refTsMustNotBeBlankWhenProvided() {
        assertThrows(IllegalArgumentException.class, () -> new TS(
                "ts-3", "AAA", null, null, "t-3", "s-3", null, null, null,
                null, null, 100L, 10L, 10L, TT.B, ActType.A01, MStatus.U, false, false, null, " "
        ));
    }
}
