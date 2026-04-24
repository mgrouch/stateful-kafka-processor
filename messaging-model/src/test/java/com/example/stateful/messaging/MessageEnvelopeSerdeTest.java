package com.example.stateful.messaging;

import com.example.stateful.domain.T;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageEnvelopeSerdeTest {

    @Test
    void tEnvelopeRoundTripsWithRefAndCancel() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        MessageEnvelope original = MessageEnvelope.forT(new T("t-1", "IBM", "REF-10", true, 77L));

        String json = mapper.writeValueAsString(original);
        MessageEnvelope parsed = mapper.readValue(json, MessageEnvelope.class);

        assertEquals(MessageKind.T, parsed.kind());
        assertEquals("t-1", parsed.t().id());
        assertEquals("IBM", parsed.t().pid());
        assertEquals("REF-10", parsed.t().ref());
        assertTrue(parsed.t().cancel());
        assertEquals(77L, parsed.t().q());
    }
}
