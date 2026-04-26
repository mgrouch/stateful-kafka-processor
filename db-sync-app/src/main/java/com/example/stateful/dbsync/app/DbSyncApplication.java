package com.example.stateful.dbsync.app;

import com.example.stateful.dbsync.config.DbSyncSettings;
import com.example.stateful.dbsync.consumer.DbSyncService;
import com.example.stateful.dbsync.repository.DbSyncRepository;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.support.GenericApplicationContext;

import javax.sql.DataSource;

public final class DbSyncApplication {

    private DbSyncApplication() {
    }

    public static void main(String[] args) {
        createApplication().run(args);
    }

    public static SpringApplication createApplication() {
        SpringApplication application = new SpringApplication(DbSyncBootstrap.class);
        application.setWebApplicationType(WebApplicationType.NONE);
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
        context.registerBean(DbSyncSettings.class, () -> DbSyncSettings.from(context.getEnvironment()));
        context.registerBean(DataSource.class, () -> {
            DbSyncSettings settings = context.getBean(DbSyncSettings.class);
            return DataSourceBuilder.create()
                    .url(settings.dbUrl())
                    .username(settings.dbUser())
                    .password(settings.dbPassword())
                    .build();
        });
        context.registerBean(DbSyncRepository.class, () -> new DbSyncRepository(context.getBean(DataSource.class)));
        context.registerBean(DbSyncService.class, () -> new DbSyncService(
                context.getBean(DbSyncSettings.class),
                context.getBean(ObjectMapper.class),
                context.getBean(DbSyncRepository.class)
        ));
        context.registerBean(ApplicationRunner.class, () -> args -> context.getBean(DbSyncService.class).runForever());
    }

    public static final class DbSyncBootstrap {
    }
}
