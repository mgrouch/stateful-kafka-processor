package com.example.stateful.messaging;

import com.example.stateful.domain.S;
import com.example.stateful.domain.TS;

import java.util.Objects;

public record MessageEnvelope(MessageKind kind, S s, TS ts) {

    public MessageEnvelope {
        Objects.requireNonNull(kind, "kind must not be null");
        validate(kind, s, ts);
    }

    public static MessageEnvelope forS(S value) {
        return new MessageEnvelope(MessageKind.S, Objects.requireNonNull(value), null);
    }

    public static MessageEnvelope forTS(TS value) {
        return new MessageEnvelope(MessageKind.TS, null, Objects.requireNonNull(value));
    }

    public String partitionKey() {
        return switch (kind) {
            case S -> s.pid();
            case TS -> ts.pid();
        };
    }

    private static void validate(MessageKind kind, S s, TS ts) {
        int present = (s != null ? 1 : 0) + (ts != null ? 1 : 0);
        if (present != 1) {
            throw new IllegalArgumentException("Exactly one payload must be present");
        }
        switch (kind) {
            case S -> {
                if (s == null || ts != null) {
                    throw new IllegalArgumentException("kind S requires only field s");
                }
            }
            case TS -> {
                if (ts == null || s != null) {
                    throw new IllegalArgumentException("kind TS requires only field ts");
                }
            }
        }
    }
}
