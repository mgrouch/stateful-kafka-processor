package com.example.stateful.processor.processor;

import com.example.stateful.domain.S;

public record SignedSupplyUsage(long remainingCarry, long remainingRegular) {

    public static SignedSupplyUsage supplyUsage(S s) {
        long sign = Long.signum(s.q() + s.q_carry());
        long carryMagnitude = Math.abs(s.q_carry());
        long regularMagnitude = Math.abs(s.q());
        long usedMagnitude = Math.abs(s.q_a());

        long carryUsedMagnitude = Math.min(usedMagnitude, carryMagnitude);
        long regularUsedMagnitude = Math.min(Math.max(usedMagnitude - carryMagnitude, 0L), regularMagnitude);

        long remainingCarry = sign * (carryMagnitude - carryUsedMagnitude);
        long remainingRegular = sign * (regularMagnitude - regularUsedMagnitude);
        return new SignedSupplyUsage(remainingCarry, remainingRegular);
    }
}
