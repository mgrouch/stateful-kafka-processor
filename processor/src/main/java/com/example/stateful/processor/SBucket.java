package com.example.stateful.processor;

import com.example.stateful.domain.S;

import java.util.ArrayList;
import java.util.List;

public record SBucket(List<S> items) {

    public SBucket {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public static SBucket empty() {
        return new SBucket(List.of());
    }

    public SBucket append(S value) {
        ArrayList<S> copy = new ArrayList<>(items);
        copy.add(value);
        return new SBucket(copy);
    }
}
