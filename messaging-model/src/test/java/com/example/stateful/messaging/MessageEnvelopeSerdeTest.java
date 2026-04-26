package com.example.stateful.messaging;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageEnvelopeSerdeTest {

    @Test
    void tEnvelopeRoundTripsWithRefCancelAndAllocationState() {
        MessageEnvelope original = MessageEnvelope.forT(new T("t-1", "AAA", "REF-10", null, null, null, null, AStatus.FAIL, true, 77L, 11L, 0L, 9L));

        byte[] bytes = MessageEnvelopeAvroCodec.serialize(original);
        MessageEnvelope parsed = MessageEnvelopeAvroCodec.deserialize(bytes);

        assertEquals(MessageKind.T, parsed.kind());
        assertEquals("t-1", parsed.t().id());
        assertEquals("AAA", parsed.t().pid());
        assertEquals("REF-10", parsed.t().ref());
        assertTrue(parsed.t().cancel());
        assertEquals(77L, parsed.t().q());
        assertEquals(11L, parsed.t().q_a_total());
        assertEquals(0L, parsed.t().q_a_delta_last());
        assertEquals(9L, parsed.t().q_f());
        assertEquals(AStatus.FAIL, parsed.t().a_status());
    }

    @Test
    void sEnvelopeRoundTripsWithRolloverFlag() {
        MessageEnvelope original = MessageEnvelope.forS(new S("s-1", "AAA", 91L, 9L, 8L, true));

        byte[] bytes = MessageEnvelopeAvroCodec.serialize(original);
        MessageEnvelope parsed = MessageEnvelopeAvroCodec.deserialize(bytes);

        assertEquals(MessageKind.S, parsed.kind());
        assertEquals("s-1", parsed.s().id());
        assertEquals("AAA", parsed.s().pid());
        assertEquals(91L, parsed.s().q());
        assertEquals(9L, parsed.s().q_carry());
        assertEquals(8L, parsed.s().q_a());
        assertTrue(parsed.s().rollover());
        assertFalse(parsed.s().o());
    }

    @Test
    void tsEnvelopeRoundTripsAllocatedQuantity() {
        MessageEnvelope original = MessageEnvelope.forTS(new TS("ts-1", "AAA", "t-1", "s-1", 99L, 12L));

        byte[] bytes = MessageEnvelopeAvroCodec.serialize(original);
        MessageEnvelope parsed = MessageEnvelopeAvroCodec.deserialize(bytes);

        assertEquals(MessageKind.TS, parsed.kind());
        assertEquals("AAA", parsed.ts().pid());
        assertEquals("t-1", parsed.ts().tid());
        assertEquals("s-1", parsed.ts().sid());
        assertEquals(12L, parsed.ts().q_a_delta());
        assertEquals(12L, parsed.ts().q_a_total_after());
        assertFalse(parsed.ts().o());
        assertFalse(parsed.ts().cancel());
    }
}
