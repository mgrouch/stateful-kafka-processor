package com.example.stateful.messaging;

import com.example.stateful.domain.ActType;
import com.example.stateful.domain.Dir;
import com.example.stateful.domain.MStatus;
import com.example.stateful.domain.S;
import com.example.stateful.domain.SCycle;
import com.example.stateful.domain.TS;
import com.example.stateful.domain.TT;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class MessageEnvelopeAvroCodec {
    private MessageEnvelopeAvroCodec() {}

    public static byte[] serialize(MessageEnvelope envelope) {
        if (envelope == null) return null;
        SpecificDatumWriter<com.example.stateful.messaging.avro.MessageEnvelope> writer = new SpecificDatumWriter<>(com.example.stateful.messaging.avro.MessageEnvelope.class);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(toAvro(envelope), encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize MessageEnvelope as Avro", e);
        }
    }

    public static MessageEnvelope deserialize(byte[] bytes) {
        if (bytes == null) return null;
        SpecificDatumReader<com.example.stateful.messaging.avro.MessageEnvelope> reader = new SpecificDatumReader<>(com.example.stateful.messaging.avro.MessageEnvelope.class);
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return fromAvro(reader.read(null, decoder));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize MessageEnvelope Avro", e);
        }
    }

    private static com.example.stateful.messaging.avro.MessageEnvelope toAvro(MessageEnvelope envelope) {
        return com.example.stateful.messaging.avro.MessageEnvelope.newBuilder()
                .setKind(com.example.stateful.messaging.avro.MessageKind.valueOf(envelope.kind().name()))
                .setS(envelope.s() == null ? null : toAvro(envelope.s()))
                .setTs(envelope.ts() == null ? null : toAvro(envelope.ts()))
                .build();
    }

    private static MessageEnvelope fromAvro(com.example.stateful.messaging.avro.MessageEnvelope avro) {
        return new MessageEnvelope(
                MessageKind.valueOf(avro.getKind().name()),
                avro.getS() == null ? null : fromAvro(avro.getS()),
                avro.getTs() == null ? null : fromAvro(avro.getTs())
        );
    }

    private static com.example.stateful.messaging.avro.S toAvro(S s) {
        return com.example.stateful.messaging.avro.S.newBuilder()
                .setId(s.id()).setPid(s.pid()).setRefS(s.refS()).setBDate(s.bDate()).setQ(s.q()).setQCarry(s.q_carry()).setQA(s.q_a())
                .setQAOppositeDelta(s.q_a_opposite_delta()).setQAOppositeTotal(s.q_a_opposite_total())
                .setRollover(s.rollover()).setO(s.o())
                .setDir(com.example.stateful.messaging.avro.Dir.valueOf(s.dir().name()))
                .setSCycle(com.example.stateful.messaging.avro.SCycle.valueOf(s.sCycle().name()))
                .setLedgerTime(s.ledgerTime())
                .build();
    }

    private static S fromAvro(com.example.stateful.messaging.avro.S s) {
        return new S(s.getId(), s.getPid(), s.getBDate(), s.getQ(), s.getQCarry(), s.getQA(), s.getQAOppositeDelta(), s.getQAOppositeTotal(), s.getRollover(), s.getO(), Dir.valueOf(s.getDir().name()), SCycle.valueOf(s.getSCycle().name()), s.getLedgerTime(), nullable(s.getRefS()));
    }

    private static com.example.stateful.messaging.avro.TS toAvro(TS ts) {
        return com.example.stateful.messaging.avro.TS.newBuilder()
                .setId(ts.id()).setPid(ts.pid()).setRefTS(ts.refTS()).setPidAlt1(ts.pidAlt1()).setPidAlt2(ts.pidAlt2())
                .setTid(ts.tid()).setSid(ts.sid()).setAccId(ts.accId()).setSorId(ts.sorId()).setOarId(ts.oarId())
                .setTDate(ts.tDate()).setSDate(ts.sDate()).setQ(ts.q()).setQADelta(ts.q_a_delta()).setQATotalAfter(ts.q_a_total_after())
                .setTt(com.example.stateful.messaging.avro.TT.valueOf(ts.tt().name()))
                .setActivity(com.example.stateful.messaging.avro.ActType.valueOf(ts.activity().name()))
                .setMStatus(com.example.stateful.messaging.avro.MStatus.valueOf(ts.mStatus().name()))
                .setO(ts.o()).setCancel(ts.cancel())
                .setExtraS(ts.extraS() == null ? null : ts.extraS().stream().map(MessageEnvelopeAvroCodec::toAvro).toList())
                .build();
    }

    private static TS fromAvro(com.example.stateful.messaging.avro.TS ts) {
        return new TS(ts.getId(), ts.getPid(), nullable(ts.getPidAlt1()), nullable(ts.getPidAlt2()), ts.getTid(), ts.getSid(), nullable(ts.getAccId()), nullable(ts.getSorId()), nullable(ts.getOarId()),
                ts.getTDate(), ts.getSDate(), ts.getQ(), ts.getQADelta(), ts.getQATotalAfter(), TT.valueOf(ts.getTt().name()), ActType.valueOf(ts.getActivity().name()), MStatus.valueOf(ts.getMStatus().name()), ts.getO(), ts.getCancel(),
                ts.getExtraS() == null ? null : ts.getExtraS().stream().map(MessageEnvelopeAvroCodec::fromAvro).toList(), nullable(ts.getRefTS()));
    }

    private static String nullable(CharSequence value) {
        return value == null ? null : value.toString();
    }
}
