package com.example.stateful.dbsync;

import java.sql.Connection;
import java.sql.SQLException;

public record TempTableSql(
        String createAcceptedTTemp,
        String createAcceptedSTemp,
        String createGeneratedTsTemp,
        String createUpsertUnprocessedTTemp,
        String createUpsertUnprocessedSTemp,
        String createDeleteUnprocessedTTemp,
        String createDeleteUnprocessedSTemp,
        String createOffsetsTemp,
        String insertAcceptedTTemp,
        String insertAcceptedSTemp,
        String insertGeneratedTsTemp,
        String insertUpsertUnprocessedTTemp,
        String insertUpsertUnprocessedSTemp,
        String insertDeleteUnprocessedTTemp,
        String insertDeleteUnprocessedSTemp,
        String insertOffsetsTemp,
        String mergeAcceptedT,
        String mergeAcceptedS,
        String mergeGeneratedTs,
        String mergeUnprocessedT,
        String mergeUnprocessedS,
        String deleteUnprocessedT,
        String deleteUnprocessedS,
        String mergeOffsets
) {

    public static TempTableSql forConnection(Connection connection) throws SQLException {
        String databaseName = connection.getMetaData().getDatabaseProductName().toLowerCase();
        if (databaseName.contains("microsoft sql server")) {
            return sqlServer();
        }
        return h2();
    }

    private static TempTableSql h2() {
        return new TempTableSql(
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS accepted_t_tmp (id VARCHAR(128), pid VARCHAR(128), ref VARCHAR(128), cancel BOOLEAN, q BIGINT, source_topic VARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id VARCHAR(255))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS accepted_s_tmp (id VARCHAR(128), pid VARCHAR(128), q BIGINT, source_topic VARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id VARCHAR(255))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS generated_ts_tmp (id VARCHAR(128), pid VARCHAR(128), q BIGINT, source_topic VARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id VARCHAR(255))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS unprocessed_t_upsert_tmp (pid VARCHAR(128), t_id VARCHAR(128), ref VARCHAR(128), cancel BOOLEAN, q BIGINT, source_topic VARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id VARCHAR(255))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS unprocessed_s_upsert_tmp (pid VARCHAR(128), s_id VARCHAR(128), q BIGINT, source_topic VARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id VARCHAR(255))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS unprocessed_t_delete_tmp (pid VARCHAR(128))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS unprocessed_s_delete_tmp (pid VARCHAR(128))",
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS offsets_tmp (consumer_group VARCHAR(255), topic VARCHAR(255), partition_id INT, next_offset BIGINT, updated_at TIMESTAMP)",
                "INSERT INTO accepted_t_tmp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO accepted_s_tmp VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO generated_ts_tmp VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO unprocessed_t_upsert_tmp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO unprocessed_s_upsert_tmp VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO unprocessed_t_delete_tmp VALUES (?)",
                "INSERT INTO unprocessed_s_delete_tmp VALUES (?)",
                "INSERT INTO offsets_tmp VALUES (?, ?, ?, ?, ?)",
                "MERGE INTO accepted_t (id, pid, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id) KEY(id) SELECT id, pid, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id FROM accepted_t_tmp",
                "MERGE INTO accepted_s (id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id) KEY(id) SELECT id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id FROM accepted_s_tmp",
                "MERGE INTO generated_ts (id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id) KEY(id) SELECT id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id FROM generated_ts_tmp",
                "MERGE INTO unprocessed_t_state (pid, t_id, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id) KEY(pid) SELECT pid, t_id, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id FROM unprocessed_t_upsert_tmp",
                "MERGE INTO unprocessed_s_state (pid, s_id, q, source_topic, source_partition, source_offset, source_timestamp, event_id) KEY(pid) SELECT pid, s_id, q, source_topic, source_partition, source_offset, source_timestamp, event_id FROM unprocessed_s_upsert_tmp",
                "DELETE FROM unprocessed_t_state WHERE pid IN (SELECT pid FROM unprocessed_t_delete_tmp)",
                "DELETE FROM unprocessed_s_state WHERE pid IN (SELECT pid FROM unprocessed_s_delete_tmp)",
                "MERGE INTO kafka_consumer_offsets (consumer_group, topic, partition_id, next_offset, updated_at) KEY(consumer_group, topic, partition_id) SELECT consumer_group, topic, partition_id, next_offset, updated_at FROM offsets_tmp"
        );
    }

    private static TempTableSql sqlServer() {
        return new TempTableSql(
                "CREATE TABLE #accepted_t (id NVARCHAR(128), pid NVARCHAR(128), ref NVARCHAR(128), cancel BIT, q BIGINT, source_topic NVARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id NVARCHAR(255))",
                "CREATE TABLE #accepted_s (id NVARCHAR(128), pid NVARCHAR(128), q BIGINT, source_topic NVARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id NVARCHAR(255))",
                "CREATE TABLE #generated_ts (id NVARCHAR(128), pid NVARCHAR(128), q BIGINT, source_topic NVARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id NVARCHAR(255))",
                "CREATE TABLE #unprocessed_t_upsert (pid NVARCHAR(128), t_id NVARCHAR(128), ref NVARCHAR(128), cancel BIT, q BIGINT, source_topic NVARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id NVARCHAR(255))",
                "CREATE TABLE #unprocessed_s_upsert (pid NVARCHAR(128), s_id NVARCHAR(128), q BIGINT, source_topic NVARCHAR(255), source_partition INT, source_offset BIGINT, source_timestamp BIGINT, event_id NVARCHAR(255))",
                "CREATE TABLE #unprocessed_t_delete (pid NVARCHAR(128))",
                "CREATE TABLE #unprocessed_s_delete (pid NVARCHAR(128))",
                "CREATE TABLE #offsets (consumer_group NVARCHAR(255), topic NVARCHAR(255), partition_id INT, next_offset BIGINT, updated_at DATETIME2)",
                "INSERT INTO #accepted_t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO #accepted_s VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO #generated_ts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO #unprocessed_t_upsert VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO #unprocessed_s_upsert VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "INSERT INTO #unprocessed_t_delete VALUES (?)",
                "INSERT INTO #unprocessed_s_delete VALUES (?)",
                "INSERT INTO #offsets VALUES (?, ?, ?, ?, ?)",
                "INSERT INTO accepted_t (id, pid, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id) SELECT t.id, t.pid, t.ref, t.cancel, t.q, t.source_topic, t.source_partition, t.source_offset, t.source_timestamp, t.event_id FROM #accepted_t t WHERE NOT EXISTS (SELECT 1 FROM accepted_t existing WHERE existing.event_id = t.event_id)",
                "INSERT INTO accepted_s (id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id) SELECT s.id, s.pid, s.q, s.source_topic, s.source_partition, s.source_offset, s.source_timestamp, s.event_id FROM #accepted_s s WHERE NOT EXISTS (SELECT 1 FROM accepted_s existing WHERE existing.event_id = s.event_id)",
                "INSERT INTO generated_ts (id, pid, q, source_topic, source_partition, source_offset, source_timestamp, event_id) SELECT ts.id, ts.pid, ts.q, ts.source_topic, ts.source_partition, ts.source_offset, ts.source_timestamp, ts.event_id FROM #generated_ts ts WHERE NOT EXISTS (SELECT 1 FROM generated_ts existing WHERE existing.event_id = ts.event_id)",
                "MERGE unprocessed_t_state AS target USING #unprocessed_t_upsert AS src ON target.pid = src.pid WHEN MATCHED THEN UPDATE SET t_id = src.t_id, ref = src.ref, cancel = src.cancel, q = src.q, source_topic = src.source_topic, source_partition = src.source_partition, source_offset = src.source_offset, source_timestamp = src.source_timestamp, event_id = src.event_id WHEN NOT MATCHED THEN INSERT (pid, t_id, ref, cancel, q, source_topic, source_partition, source_offset, source_timestamp, event_id) VALUES (src.pid, src.t_id, src.ref, src.cancel, src.q, src.source_topic, src.source_partition, src.source_offset, src.source_timestamp, src.event_id)",
                "MERGE unprocessed_s_state AS target USING #unprocessed_s_upsert AS src ON target.pid = src.pid WHEN MATCHED THEN UPDATE SET s_id = src.s_id, q = src.q, source_topic = src.source_topic, source_partition = src.source_partition, source_offset = src.source_offset, source_timestamp = src.source_timestamp, event_id = src.event_id WHEN NOT MATCHED THEN INSERT (pid, s_id, q, source_topic, source_partition, source_offset, source_timestamp, event_id) VALUES (src.pid, src.s_id, src.q, src.source_topic, src.source_partition, src.source_offset, src.source_timestamp, src.event_id)",
                "DELETE target FROM unprocessed_t_state target INNER JOIN #unprocessed_t_delete d ON target.pid = d.pid",
                "DELETE target FROM unprocessed_s_state target INNER JOIN #unprocessed_s_delete d ON target.pid = d.pid",
                "MERGE kafka_consumer_offsets AS target USING #offsets AS src ON target.consumer_group = src.consumer_group AND target.topic = src.topic AND target.partition_id = src.partition_id WHEN MATCHED THEN UPDATE SET next_offset = src.next_offset, updated_at = src.updated_at WHEN NOT MATCHED THEN INSERT (consumer_group, topic, partition_id, next_offset, updated_at) VALUES (src.consumer_group, src.topic, src.partition_id, src.next_offset, src.updated_at)"
        );
    }
}
