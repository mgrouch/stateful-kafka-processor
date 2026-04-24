package com.example.stateful.processor;

import com.example.stateful.messaging.DbSyncEnvelope;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

public final class DbSyncWriterLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DbSyncWriterLifecycle.class);

    private final DbSyncWriterSettings settings;
    private final DbSyncWriter writer;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread workerThread;
    private KafkaConsumer<String, DbSyncEnvelope> consumer;

    public DbSyncWriterLifecycle(DbSyncWriterSettings settings, SerdeFactory serdeFactory, DbSyncSqlRepository repository) {
        this.settings = settings;
        this.writer = new DbSyncWriter(settings, serdeFactory, repository);
    }

    @Override
    public void start() {
        if (!settings.enabled() || !running.compareAndSet(false, true)) {
            return;
        }
        this.consumer = writer.createConsumer();
        writer.initialize(consumer);

        this.workerThread = new Thread(() -> {
            while (running.get()) {
                writer.pollAndApplyOnce(consumer);
            }
        }, "db-sync-writer");
        this.workerThread.start();
        log.info("Started db sync writer topic={} consumerGroup={}", settings.topic(), settings.consumerGroup());
    }

    @Override
    public void stop() {
        stop(() -> { });
    }

    @Override
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();
            return;
        }
        try {
            if (consumer != null) {
                consumer.wakeup();
            }
            if (workerThread != null) {
                workerThread.join(3000);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptedException);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return settings.enabled();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }
}
