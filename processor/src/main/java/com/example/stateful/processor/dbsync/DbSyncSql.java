package com.example.stateful.processor.dbsync;

public final class DbSyncSql {

    private DbSyncSql() {
    }

    public static final String CREATE_SCHEMA = """
            CREATE TABLE IF NOT EXISTS accepted_t (
                id VARCHAR(128) PRIMARY KEY,
                pid VARCHAR(128) NOT NULL,
                ref VARCHAR(128) NOT NULL,
                cancel BOOLEAN NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS accepted_s (
                id VARCHAR(128) PRIMARY KEY,
                pid VARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS generated_ts (
                id VARCHAR(128) PRIMARY KEY,
                pid VARCHAR(128) NOT NULL,
                tid VARCHAR(128) NOT NULL,
                sid VARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS unprocessed_t_state (
                pid VARCHAR(128) PRIMARY KEY,
                t_id VARCHAR(128) NOT NULL,
                ref VARCHAR(128) NOT NULL,
                cancel BOOLEAN NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS unprocessed_s_state (
                pid VARCHAR(128) PRIMARY KEY,
                s_id VARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS kafka_consumer_offsets (
                topic VARCHAR(255) NOT NULL,
                partition_id INT NOT NULL,
                next_offset BIGINT NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (topic, partition_id)
            );
            """;
}
