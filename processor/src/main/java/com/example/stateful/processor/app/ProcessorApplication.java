package com.example.stateful.processor.app;

import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.dbsync.DbSyncWriter;
import com.example.stateful.processor.dbsync.DbSyncWriterSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.stream.KafkaStreamsManager;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.support.GenericApplicationContext;

import javax.sql.DataSource;
import java.util.Map;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public final class ProcessorApplication {

    private ProcessorApplication() {
    }

    public static void main(String[] args) {
        createApplication().run(args);
    }

    public static SpringApplication createApplication() {
        SpringApplication application = new SpringApplication(ProcessorBootstrap.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setDefaultProperties(Map.of(
                "app.application-id", "stateful-data-processor",
                "app.input-topic", "input-events",
                "app.output-topic", "processed-events",
                "app.db-sync-topic", "db-sync-events",
                "app.state-dir", "processor/kafka-streams-state",
                "app.commit-interval-ms", "100",
                "app.streams.replication-factor", "3",
                "app.streams.num-standby-replicas", "1",
                "spring.kafka.bootstrap-servers", "localhost:9092"
        ));
        application.addInitializers(context -> registerBeans((GenericApplicationContext) context));
        return application;
    }

    private static void registerBeans(GenericApplicationContext context) {
        context.registerBean(ObjectMapper.class, () -> {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.findAndRegisterModules();
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return objectMapper;
        });
        context.registerBean(ProcessorSettings.class, () -> ProcessorSettings.from(context.getEnvironment()));
        context.registerBean(SerdeFactory.class, () -> new SerdeFactory(context.getBean(ObjectMapper.class)));
        context.registerBean(DbSyncWriterSettings.class, () -> DbSyncWriterSettings.from(context.getEnvironment(), context.getBean(ProcessorSettings.class)));
        context.registerBean(DataSource.class, () -> {
            DbSyncWriterSettings writerSettings = context.getBean(DbSyncWriterSettings.class);
            return DataSourceBuilder.create()
                    .url(writerSettings.dbUrl())
                    .username(writerSettings.dbUser())
                    .password(writerSettings.dbPassword())
                    .type(DriverManagerDataSource.class)
                    .build();
        });
        context.registerBean(DbSyncWriter.class, () -> new DbSyncWriter(
                context.getBean(DbSyncWriterSettings.class),
                context.getBean(SerdeFactory.class),
                context.getBean(DataSource.class)
        ));
        context.registerBean(KafkaStreamsManager.class, () -> new KafkaStreamsManager(
                context.getBean(ProcessorSettings.class),
                context.getBean(SerdeFactory.class)
        ));
    }

    public static final class ProcessorBootstrap {
    }
}
