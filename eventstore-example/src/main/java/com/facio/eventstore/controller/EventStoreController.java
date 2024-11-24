package com.facio.eventstore.controller;

import com.eventstore.dbclient.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class EventStoreController {

    private static final Logger logger = LoggerFactory.getLogger(EventStoreController.class);

    @Autowired
    private EventStoreDBClient eventStoreClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/events")
    public ResponseEntity<String> writeEvent(@RequestBody JsonNode payload) {
        try {
            // Log the incoming payload
            logger.info("Received payload...");

            // Check if the payload is an ObjectNode to modify it
            if (payload.isObject()) {
                logger.info("payload is Object");
                ObjectNode objectNode = (ObjectNode) payload;
                // Update the "timestamp" property with the current date
                objectNode.put("timestamp", Instant.now().toString());
            }

            // Convert the payload to a string
            String eventDataString = objectMapper.writeValueAsString(payload);

            // Create an EventData object explicitly for JSON format
            EventData eventData = EventData.builderAsJson(
                UUID.randomUUID(), // Unique event ID
                "payments-event",  // Event type
                eventDataString    // JSON data as a string
            ).build();

            // Generate a new stream name with the prefix "payments-order"
            String streamName = "payments-order-" + UUID.randomUUID();

            // Log the stream name
            logger.info("Writing event to stream: {}", streamName);

            // Write the event to EventStore
            eventStoreClient.appendToStream(streamName, AppendToStreamOptions.get().expectedRevision(ExpectedRevision.ANY), eventData).get();

            logger.info("Event written successfully to stream: {}", streamName);
            return ResponseEntity.ok("Event written successfully to stream: " + streamName);
        } catch (Exception e) {
            logger.error("Failed to write event", e);
            return ResponseEntity.status(500).body("Failed to write event: " + e.getMessage());
        }
    }
}