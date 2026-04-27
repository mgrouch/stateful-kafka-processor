package com.example.stateful.processor.state;

public final class StateStores {

    public static final String UNPROCESSED_TS_STORE = "unprocessed-ts-store";
    public static final String UNPROCESSED_T_STORE = UNPROCESSED_TS_STORE;
    public static final String UNPROCESSED_S_STORE = "unprocessed-s-store";
    public static final String T_DEDUPE_STORE = "t-dedupe-store";

    private StateStores() {
    }
}
