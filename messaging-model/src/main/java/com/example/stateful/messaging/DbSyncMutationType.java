package com.example.stateful.messaging;

public enum DbSyncMutationType {
    ACCEPTED_T,
    ACCEPTED_S,
    GENERATED_TS,
    UPSERT_UNPROCESSED_T,
    UPSERT_UNPROCESSED_S,
    DELETE_UNPROCESSED_T,
    DELETE_UNPROCESSED_S
}
