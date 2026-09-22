package com.example.seat.producer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.seat.model.CinemaBookingRequest;
import com.example.seat.model.SeatAllocationEvent;

import lombok.RequiredArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class SeatConfirmedProducer {

    private static final String TOPIC = "seat-confirmed-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishSeatConfirmed(CinemaBookingRequest request, String correlationId) {
        SeatAllocationEvent event = new SeatAllocationEvent(
                request.getCinemaBookingId(),
                request.getMovieCode(),
                request.getSeatNumbers(),
                request.getCustomerEmail(),
                request.getTotalPrice(),
                "SEAT_CONFIRMED"
        );

        String payload = objectMapper.writeValueAsString(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, request.getCinemaBookingId(), payload);

        // Chuyen tiep correlationId sang HEADER cua su kien ke tiep
        record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);
    }
}
