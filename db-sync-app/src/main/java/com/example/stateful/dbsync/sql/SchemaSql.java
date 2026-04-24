package com.example.stateful.dbsync.sql;

import java.sql.Connection;
import java.sql.SQLException;

public final class SchemaSql {

    private SchemaSql() {
    }

    public static String createSchema(Connection connection) throws SQLException {
        String databaseName = connection.getMetaData().getDatabaseProductName().toLowerCase();
        if (databaseName.contains("microsoft sql server")) {
            return SQL_SERVER;
        }
        return H2;
    }

    private static final String H2 = """
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
                pid VARCHAR(128) NOT NULL,
                t_id VARCHAR(128) NOT NULL,
                ref VARCHAR(128) NOT NULL,
                cancel BOOLEAN NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL,
                PRIMARY KEY (pid, t_id)
            );
            CREATE TABLE IF NOT EXISTS unprocessed_s_state (
                pid VARCHAR(128) NOT NULL,
                s_id VARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic VARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id VARCHAR(255) NOT NULL,
                PRIMARY KEY (pid, s_id)
            );
            CREATE TABLE IF NOT EXISTS kafka_consumer_offsets (
                consumer_group VARCHAR(255) NOT NULL,
                topic VARCHAR(255) NOT NULL,
                partition_id INT NOT NULL,
                next_offset BIGINT NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (consumer_group, topic, partition_id)
            );
            """;

    private static final String SQL_SERVER = """
            IF OBJECT_ID('accepted_t', 'U') IS NULL
            CREATE TABLE accepted_t (
                id NVARCHAR(128) PRIMARY KEY,
                pid NVARCHAR(128) NOT NULL,
                ref NVARCHAR(128) NOT NULL,
                cancel BIT NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic NVARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id NVARCHAR(255) NOT NULL UNIQUE
            );
            IF OBJECT_ID('accepted_s', 'U') IS NULL
            CREATE TABLE accepted_s (
                id NVARCHAR(128) PRIMARY KEY,
                pid NVARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic NVARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id NVARCHAR(255) NOT NULL UNIQUE
            );
            IF OBJECT_ID('generated_ts', 'U') IS NULL
            CREATE TABLE generated_ts (
                id NVARCHAR(128) PRIMARY KEY,
                pid NVARCHAR(128) NOT NULL,
                tid NVARCHAR(128) NOT NULL,
                sid NVARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic NVARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id NVARCHAR(255) NOT NULL UNIQUE
            );
            IF OBJECT_ID('unprocessed_t_state', 'U') IS NULL
            CREATE TABLE unprocessed_t_state (
                pid NVARCHAR(128) NOT NULL,
                t_id NVARCHAR(128) NOT NULL,
                ref NVARCHAR(128) NOT NULL,
                cancel BIT NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic NVARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id NVARCHAR(255) NOT NULL,
                PRIMARY KEY (pid, t_id)
            );
            IF OBJECT_ID('unprocessed_s_state', 'U') IS NULL
            CREATE TABLE unprocessed_s_state (
                pid NVARCHAR(128) NOT NULL,
                s_id NVARCHAR(128) NOT NULL,
                q BIGINT NOT NULL,
                q_a BIGINT NOT NULL,
                source_topic NVARCHAR(255) NOT NULL,
                source_partition INT NOT NULL,
                source_offset BIGINT NOT NULL,
                source_timestamp BIGINT NOT NULL,
                event_id NVARCHAR(255) NOT NULL,
                PRIMARY KEY (pid, s_id)
            );
            IF OBJECT_ID('kafka_consumer_offsets', 'U') IS NULL
            CREATE TABLE kafka_consumer_offsets (
                consumer_group NVARCHAR(255) NOT NULL,
                topic NVARCHAR(255) NOT NULL,
                partition_id INT NOT NULL,
                next_offset BIGINT NOT NULL,
                updated_at DATETIME2 NOT NULL,
                PRIMARY KEY (consumer_group, topic, partition_id)
            );
            """;
}
