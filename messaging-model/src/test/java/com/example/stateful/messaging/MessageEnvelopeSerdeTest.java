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

        MessageEnvelope original = MessageEnvelope.forT(new T("t-1", "AAA", "REF-10", null, null, true, 77L, 11L, 0L, 9L, AStatus.FAIL, null, null));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

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
    void sEnvelopeRoundTripsWithRolloverFlag() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MessageEnvelope original = MessageEnvelope.forS(new S("s-1", "AAA", 91L, 9L, 8L, true));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.S, parsed.kind());
        assertEquals("s-1", parsed.s().id());
        assertEquals("AAA", parsed.s().pid());
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

        MessageEnvelope original = MessageEnvelope.forTS(new TS("ts-1", "AAA", "t-1", "s-1", 99L, 12L));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.TS, parsed.kind());
        assertEquals("AAA", parsed.ts().pid());
        assertEquals("t-1", parsed.ts().tid());
        assertEquals("s-1", parsed.ts().sid());
        assertEquals(12L, parsed.ts().q_a_delta());
        assertEquals(12L, parsed.ts().q_a_total_after());
    }

    @Test
    void legacyQaFieldsDeserializeIntoRenamedFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        String tJson = """
                {"kind":"T","t":{"id":"t-legacy","pid":"AAA","ref":"REF-1","cancel":false,"q":100,"q_a":40}}
                """;
        MessageEnvelope parsedT = mapper.readValue(tJson, MessageEnvelope.class);
        assertEquals(40L, parsedT.t().q_a_total());
        assertEquals(0L, parsedT.t().q_a_delta_last());

        String tsJson = """
                {"kind":"TS","ts":{"id":"ts-legacy","pid":"AAA","tid":"t-legacy","sid":"s-legacy","q":100,"q_a_delta":30,"q_a":70}}
                """;
        MessageEnvelope parsedTs = mapper.readValue(tsJson, MessageEnvelope.class);
        assertEquals(30L, parsedTs.ts().q_a_delta());
        assertEquals(70L, parsedTs.ts().q_a_total_after());
    }
}
