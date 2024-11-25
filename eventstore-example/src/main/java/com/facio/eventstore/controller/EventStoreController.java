package com.facio.eventstore.controller;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.DeleteResult;
import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
            eventStoreClient.appendToStream(streamName, AppendToStreamOptions.get().expectedRevision(ExpectedRevision.any()), eventData).get();

            logger.info("Event written successfully to stream: {}", streamName);
            return ResponseEntity.ok("Event written successfully to stream: " + streamName);
        } catch (Exception e) {
            logger.error("Failed to write event", e);
            return ResponseEntity.status(500).body("Failed to write event: " + e.getMessage());
        }
    }

    @GetMapping("/streams")
    public ResponseEntity<List<String>> listAllStreams() {
        List<String> streamNames = new ArrayList<>();
        try {
            logger.info("Reading from $streams system stream...");
    
            // Read events from the $streams system stream
            ReadStreamOptions options = ReadStreamOptions.get()
                    .forwards()
                    .fromStart();
    
            ReadResult result = eventStoreClient.readStream("$streams", options).get();
    
            // Extract stream names from the events
            for (ResolvedEvent resolvedEvent : result.getEvents()) {
                RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
                String streamName = new String(recordedEvent.getEventData()); // Convert byte array to string
                String realStreamName = streamName.replace("0@", "");

                // Check if the stream exists by attempting to read from it
                try {
                    ReadStreamOptions streamOptions = ReadStreamOptions.get()
                            .forwards()
                            .fromStart();
                    eventStoreClient.readStream(realStreamName, streamOptions).get();
                    logger.info("Found active stream: {}", realStreamName);
                    streamNames.add(realStreamName);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof StreamNotFoundException) {
                        logger.info("Stream {} is deleted or does not exist, skipping.", realStreamName);
                    } else {
                        logger.warn("Failed to read stream {}, assuming it is active.", realStreamName, e);
                    }
                }
            }
    
            logger.info("Successfully fetched {} active streams.", streamNames.size());
            return ResponseEntity.ok(streamNames);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Failed to list streams", e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/streams/delete-half")
    public ResponseEntity<String> deleteHalfOfStreams() {
        try {
            logger.info("Reading from $streams system stream...");
    
            // Read events from the $streams system stream
            ReadStreamOptions options = ReadStreamOptions.get()
                    .forwards()
                    .fromStart();
    
            ReadResult result = eventStoreClient.readStream("$streams", options).get();
    
            List<String> streamNames = new ArrayList<>();
            // Extract stream names from the events
            for (ResolvedEvent resolvedEvent : result.getEvents()) {
                RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
                String streamName = new String(recordedEvent.getEventData()); // Convert byte array to string
                String realStreamName = streamName.replace("0@", "");

                // Check if the stream exists by attempting to read from it
                try {
                    ReadStreamOptions streamOptions = ReadStreamOptions.get()
                            .forwards()
                            .fromStart();
                    eventStoreClient.readStream(realStreamName, streamOptions).get();
                    logger.info("Found active stream: {}", realStreamName);
                    streamNames.add(realStreamName);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof StreamNotFoundException) {
                        logger.info("Stream {} is deleted or does not exist, skipping.", realStreamName);
                    } else {
                        logger.warn("Failed to read stream {}, assuming it is active.", realStreamName, e);
                    }
                }
            }
    
            if (streamNames.isEmpty()) {
                logger.info("No streams found.");
                return ResponseEntity.ok("No streams to delete.");
            }
    
            // Calculate half of the streams
            int halfSize = streamNames.size() / 2;
            List<String> streamsToDelete = streamNames.subList(0, halfSize);
    
            logger.info("Deleting {} streams...", streamsToDelete.size());
    
            // Delete the first half of the streams
            for (String streamName : streamsToDelete) {
                logger.info("Deleting stream: {}", streamName);
                //soft DElete
                //eventStoreClient.deleteStream(streamName, DeleteStreamOptions.get()).get();

                //Hard Delete
                eventStoreClient.tombstoneStream(streamName, DeleteStreamOptions.get());
            }
    
            logger.info("Successfully deleted {} streams.", streamsToDelete.size());
            return ResponseEntity.ok("Successfully deleted " + streamsToDelete.size() + " streams.");
        } catch (Exception e) {
            logger.error("Failed to delete streams", e);
            return ResponseEntity.status(500).body("Failed to delete streams: " + e.getMessage());
        }
    }

    @GetMapping("/streams/delete-old")
    public ResponseEntity<String> deleteOldStreams() {
        try {
            logger.info("Reading from $streams system stream...");
    
            // Read events from the $streams system stream
            ReadStreamOptions options = ReadStreamOptions.get()
                    .forwards()
                    .fromStart();
    
            ReadResult result = eventStoreClient.readStream("$streams", options).get();
    
            List<String> streamsToDelete = new ArrayList<>();
            Instant twelveHoursAgo = Instant.now().minusSeconds(12 * 60 * 60); // 12 hours ago
    
            // Extract stream names from the events
            for (ResolvedEvent resolvedEvent : result.getEvents()) {
                RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
                String streamName = new String(recordedEvent.getEventData()); // Convert byte array to string
                String realStreamName = streamName.replace("0@", "");
    
                // Check if the stream exists and is older than 12 hours
                try {
                    ReadStreamOptions streamOptions = ReadStreamOptions.get()
                            .forwards()
                            .fromStart();
                    ReadResult streamResult = eventStoreClient.readStream(realStreamName, streamOptions).get();
    
                    // Check the creation time of the first event in the stream
                    if (!streamResult.getEvents().isEmpty()) {
                        ResolvedEvent firstEvent = streamResult.getEvents().get(0);
                        Instant streamCreationTime = firstEvent.getOriginalEvent().getCreated();
    
                        if (streamCreationTime.isBefore(twelveHoursAgo)) {
                            logger.info("Stream {} is older than 12 hours, marking for deletion.", realStreamName);
                            streamsToDelete.add(realStreamName);
                        } else {
                            logger.info("Stream {} is not older than 12 hours, skipping.", realStreamName);
                        }
                    }
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof StreamNotFoundException) {
                        logger.info("Stream {} is deleted or does not exist, skipping.", realStreamName);
                    } else {
                        logger.warn("Failed to read stream {}, assuming it is active.", realStreamName, e);
                    }
                }
            }
    
            if (streamsToDelete.isEmpty()) {
                logger.info("No streams older than 12 hours found.");
                return ResponseEntity.ok("No streams to delete.");
            }
    
            logger.info("Deleting {} streams...", streamsToDelete.size());
    
            // Delete the streams older than 12 hours
            for (String streamName : streamsToDelete) {
                logger.info("Deleting stream: {}", streamName);
                // Hard delete the stream
                eventStoreClient.tombstoneStream(streamName, DeleteStreamOptions.get()).get();
            }
    
            logger.info("Successfully deleted {} streams.", streamsToDelete.size());
            return ResponseEntity.ok("Successfully deleted " + streamsToDelete.size() + " streams.");
        } catch (Exception e) {
            logger.error("Failed to delete streams", e);
            return ResponseEntity.status(500).body("Failed to delete streams: " + e.getMessage());
        }
    }    
}