#!/bin/bash

# Project variables
PROJECT_NAME="eventstore-example"
PACKAGE_NAME="com.example.eventstore"
EVENTSTORE_VERSION="8.0.2"
SPRING_BOOT_VERSION="1.5.22.RELEASE"

# Create project directory
mkdir $PROJECT_NAME
cd $PROJECT_NAME

# Create Maven project structure
mkdir -p src/main/java/com/example/eventstore/config
mkdir -p src/main/java/com/example/eventstore/controller
mkdir -p src/main/resources

# Create pom.xml
cat <<EOL > pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>$PROJECT_NAME</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>$SPRING_BOOT_VERSION</version>
    </parent>

    <dependencies>
        <!-- Spring Boot Web -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- EventStore Client -->
        <dependency>
            <groupId>com.geteventstore</groupId>
            <artifactId>eventstore-client_2.13</artifactId>
            <version>$EVENTSTORE_VERSION</version>
        </dependency>

        <!-- Jackson for JSON processing -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- Lombok (optional) -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
EOL

# Create EventStoreConfig.java
cat <<EOL > src/main/java/com/example/eventstore/config/EventStoreConfig.java
package com.example.eventstore.config;

import eventstore.j.EsConnection;
import eventstore.j.EsConnectionFactory;
import eventstore.j.Settings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventStoreConfig {

    @Bean
    public EsConnection eventStoreConnection() {
        // Configure the EventStore connection settings
        Settings settings = Settings.Default();
        return EsConnectionFactory.create(settings);
    }
}
EOL

# Create EventStoreController.java
cat <<EOL > src/main/java/com/example/eventstore/controller/EventStoreController.java
package com.example.eventstore.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eventstore.j.EsConnection;
import eventstore.j.WriteEvents;
import eventstore.core.EventData;
import eventstore.core.ExpectedVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@RestController
@RequestMapping("/api/events")
public class EventStoreController {

    @Autowired
    private EsConnection eventStoreConnection;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping
    public ResponseEntity<String> writeEvent(@RequestBody JsonNode payload) {
        try {
            // Convert the payload to a string
            String eventDataString = objectMapper.writeValueAsString(payload);

            // Create an EventData object
            EventData eventData = new EventData(
                    UUID.randomUUID(),
                    "customer-order-event",
                    false,
                    eventDataString.getBytes(StandardCharsets.UTF_8),
                    new byte[0]
            );

            // Write the event to EventStore
            WriteEvents writeEvents = new WriteEvents(
                    "customer-order-stream", // Stream name
                    ExpectedVersion.Any(),
                    new EventData[]{eventData},
                    false
            );
            eventStoreConnection.writeEvents(writeEvents).await();

            return ResponseEntity.ok("Event written successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Failed to write event: " + e.getMessage());
        }
    }
}
EOL

# Create application.properties
cat <<EOL > src/main/resources/application.properties
# EventStore connection settings
eventstore.host=localhost
eventstore.port=1113
EOL
