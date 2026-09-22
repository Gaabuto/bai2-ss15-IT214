package com.example.movie.service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.movie.model.CinemaBookingRequest;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Service
@Slf4j
@RequiredArgsConstructor
public class BookingPublisherService {

    private static final String TOPIC = "booking-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public String createBooking(CinemaBookingRequest request) {
        // Buoc 1: sinh UUID ngau nhien ngay khi nhan request
        String correlationId = UUID.randomUUID().toString();

        log.info("[MovieBookingService] Created booking {}. CorrelationID: {}",
                request.getCinemaBookingId(), correlationId);

        String payload = objectMapper.writeValueAsString(request);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, request.getCinemaBookingId(), payload);

        // Buoc 2: gan correlationId vao HEADER, khong dua vao payload
        record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);

        return correlationId;
    }
}
