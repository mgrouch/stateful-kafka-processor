package com.example.stateful.messaging;

import com.example.stateful.domain.AStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageEnvelopeSerdeTest {

    @Test
    void tEnvelopeRoundTripsWithRefCancelAndAllocationState() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MessageEnvelope original = MessageEnvelope.forT(new T("t-1", "IBM", "REF-10", true, 77L, 11L, AStatus.FAIL));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.T, parsed.kind());
        assertEquals("t-1", parsed.t().id());
        assertEquals("IBM", parsed.t().pid());
        assertEquals("REF-10", parsed.t().ref());
        assertTrue(parsed.t().cancel());
        assertEquals(77L, parsed.t().q());
        assertEquals(11L, parsed.t().q_a());
        assertEquals(AStatus.FAIL, parsed.t().a_status());
    }

    @Test
    void sEnvelopeRoundTripsWithRolloverFlag() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MessageEnvelope original = MessageEnvelope.forS(new S("s-1", "IBM", 91L, 9L, 8L, true));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.S, parsed.kind());
        assertEquals("s-1", parsed.s().id());
        assertEquals("IBM", parsed.s().pid());
        assertEquals(91L, parsed.s().q());
        assertEquals(9L, parsed.s().q_carry());
        assertEquals(8L, parsed.s().q_a());
        assertTrue(parsed.s().rollover());
    }

    @Test
    void tsEnvelopeRoundTripsAllocatedQuantity() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MessageEnvelope original = MessageEnvelope.forTS(new TS("ts-1", "IBM", "t-1", "s-1", 99L, 12L));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.TS, parsed.kind());
        assertEquals("IBM", parsed.ts().pid());
        assertEquals("t-1", parsed.ts().tid());
        assertEquals("s-1", parsed.ts().sid());
        assertEquals(12L, parsed.ts().q_a());
    }
}
