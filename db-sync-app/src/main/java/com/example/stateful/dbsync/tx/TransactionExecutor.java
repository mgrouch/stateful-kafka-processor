package com.example.stateful.dbsync.tx;

import javax.sql.DataSource;
import java.sql.Connection;

public final class TransactionExecutor {

    private final DataSource dataSource;

    public TransactionExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void runInTransaction(SqlWork work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                work.run(connection);
                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Failed applying db-sync batch. Service must be restarted after fixing the cause.", exception);
        }
    }

    @FunctionalInterface
    public interface SqlWork {
        void run(Connection connection) throws Exception;
    }
}
