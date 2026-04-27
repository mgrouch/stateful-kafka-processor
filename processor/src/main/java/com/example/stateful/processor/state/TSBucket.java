package com.example.stateful.processor.state;

import com.example.stateful.domain.TS;

import java.util.ArrayList;
import java.util.List;

public record TSBucket(List<TS> items) {

    public TSBucket {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public static TSBucket empty() {
        return new TSBucket(List.of());
    }

    public TSBucket append(TS value) {
        ArrayList<TS> copy = new ArrayList<>(items);
        copy.add(value);
        return new TSBucket(copy);
    }

    public TSBucket withItems(List<TS> nextItems) {
        return new TSBucket(nextItems);
    }
}
