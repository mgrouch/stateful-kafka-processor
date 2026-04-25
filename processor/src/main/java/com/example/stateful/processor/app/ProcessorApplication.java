package com.example.stateful.processor.app;

import com.example.stateful.processor.config.ProcessorSettings;
import com.example.stateful.processor.serde.SerdeFactory;
import com.example.stateful.processor.stream.KafkaStreamsManager;
import com.example.stateful.processor.logic.AllocationStrategy;
import com.example.stateful.processor.logic.NaiveAlocationStrategy;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.support.GenericApplicationContext;
import java.util.Map;

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
                ProcessorSettings.REPLICATION_FACTOR_PROPERTY, String.valueOf(ProcessorSettings.DEFAULT_REPLICATION_FACTOR),
                ProcessorSettings.NUM_STANDBY_REPLICAS_PROPERTY, String.valueOf(ProcessorSettings.DEFAULT_NUM_STANDBY_REPLICAS),
                "spring.kafka.bootstrap-servers", "localhost:9093",
                "app.kafka.security-protocol", "SSL"
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
        context.registerBean(AllocationStrategy.class, () ->
                new NaiveAlocationStrategy(context.getBean(ProcessorSettings.class).allocationLotterySeed()));
        context.registerBean(KafkaStreamsManager.class, () -> new KafkaStreamsManager(
                context.getBean(ProcessorSettings.class),
                context.getBean(SerdeFactory.class),
                context.getBean(AllocationStrategy.class)
        ));
    }

    public static final class ProcessorBootstrap {
    }
}
