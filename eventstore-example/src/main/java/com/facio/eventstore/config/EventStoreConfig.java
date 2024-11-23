package com.example.eventstore.config;

import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBConnectionString;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventStoreConfig {
    @Bean
    public EventStoreDBClient eventStoreClient() throws Exception {
        // Configure the EventStore connection settings
        String connectionString = "esdb://localhost:2113?tls=false";
        return EventStoreDBClient.create(EventStoreDBConnectionString.parseOrThrow(connectionString));
    }
}