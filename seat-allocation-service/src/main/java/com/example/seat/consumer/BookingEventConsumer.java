package com.example.seat.consumer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.seat.model.CinemaBookingRequest;
import com.example.seat.producer.SeatConfirmedProducer;
import com.example.seat.service.SeatAllocationService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
@RequiredArgsConstructor
public class BookingEventConsumer {

    private final ObjectMapper objectMapper;
    private final SeatAllocationService seatAllocationService;
    private final SeatConfirmedProducer seatConfirmedProducer;

    @KafkaListener(topics = "booking-events", groupId = "seat-group")
    public void handleBooking(ConsumerRecord<String, String> record) {
        // Trich xuat correlationId tu HEADER - khong lay tu payload
        String correlationId = extractCorrelationId(record);

        log.info("[SeatAllocationService] Received SeatRequest for {}. CorrelationID: {}",
                record.key(), correlationId);

        CinemaBookingRequest request = objectMapper.readValue(record.value(), CinemaBookingRequest.class);
        String seatResult = seatAllocationService.reserveSeats(request);

        log.info("[SeatAllocationService] Seat reserved: {}. CorrelationID: {}", seatResult, correlationId);

        seatConfirmedProducer.publishSeatConfirmed(request, correlationId);
    }

    private String extractCorrelationId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("correlationId");
        if (header == null) {
            throw new IllegalStateException("Thieu correlationId trong header cua su kien booking-events");
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
