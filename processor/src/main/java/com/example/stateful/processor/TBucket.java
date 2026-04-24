package com.example.stateful.processor;

import com.example.stateful.domain.T;

import java.util.ArrayList;
import java.util.List;

public record TBucket(List<T> items) {

    public TBucket {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public static TBucket empty() {
        return new TBucket(List.of());
    }

    public TBucket append(T value) {
        ArrayList<T> copy = new ArrayList<>(items);
        copy.add(value);
        return new TBucket(copy);
    }
}
