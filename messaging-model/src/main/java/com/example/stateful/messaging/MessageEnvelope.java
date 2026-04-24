package com.example.stateful.messaging;

import com.example.stateful.domain.S;
import com.example.stateful.domain.T;
import com.example.stateful.domain.TS;

import java.util.Objects;

public record MessageEnvelope(MessageKind kind, T t, S s, TS ts) {

    public MessageEnvelope {
        Objects.requireNonNull(kind, "kind must not be null");
        validate(kind, t, s, ts);
    }

    public static MessageEnvelope forT(T value) {
        return new MessageEnvelope(MessageKind.T, Objects.requireNonNull(value), null, null);
    }

    public static MessageEnvelope forS(S value) {
        return new MessageEnvelope(MessageKind.S, null, Objects.requireNonNull(value), null);
    }

    public static MessageEnvelope forTS(TS value) {
        return new MessageEnvelope(MessageKind.TS, null, null, Objects.requireNonNull(value));
    }

    public String partitionKey() {
        return switch (kind) {
            case T -> t.pid();
            case S -> s.pid();
            case TS -> ts.pid();
        };
    }

    private static void validate(MessageKind kind, T t, S s, TS ts) {
        int present = (t != null ? 1 : 0) + (s != null ? 1 : 0) + (ts != null ? 1 : 0);
        if (present != 1) {
            throw new IllegalArgumentException("Exactly one payload must be present");
        }
        switch (kind) {
            case T -> {
                if (t == null || s != null || ts != null) {
                    throw new IllegalArgumentException("kind T requires only field t");
                }
            }
            case S -> {
                if (s == null || t != null || ts != null) {
                    throw new IllegalArgumentException("kind S requires only field s");
                }
            }
            case TS -> {
                if (ts == null || t != null || s != null) {
                    throw new IllegalArgumentException("kind TS requires only field ts");
                }
            }
        }
    }
}
