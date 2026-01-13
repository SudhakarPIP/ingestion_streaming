package com.example.ingestion.controller;

import com.example.ingestion.dto.EventRequest;
import com.example.ingestion.dto.EventResponse;
import com.example.ingestion.service.EventService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/events")
@Slf4j
public class EventController {

    private final EventService eventService;

    public EventController(EventService eventService) {
        this.eventService = eventService;
    }

    @PostMapping
    public ResponseEntity<EventResponse> ingestEvent(@Valid @RequestBody EventRequest request) {
        log.info("Received event ingestion request: tenantId={}, eventId={}", 
            request.tenantId(), request.eventId());
        EventResponse response = eventService.ingestEvent(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/bulk")
    public ResponseEntity<List<EventResponse>> ingestEvents(@Valid @RequestBody List<EventRequest> requests) {
        log.info("Received bulk event ingestion request: {} events", requests.size());
        List<EventResponse> responses = eventService.ingestEvents(requests);
        return ResponseEntity.status(HttpStatus.CREATED).body(responses);
    }

    @GetMapping("/{tenantId}/{eventId}")
    public ResponseEntity<EventResponse> getEvent(@PathVariable String tenantId, @PathVariable String eventId) {
        log.info("Getting event: tenantId={}, eventId={}", tenantId, eventId);

        EventResponse response = eventService.getEvent(tenantId, eventId);
        return ResponseEntity.ok(response);
    }
}
