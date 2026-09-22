package com.example.payment.consumer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.payment.model.PaymentEvent;
import com.example.payment.model.SeatAllocationEvent;
import com.example.payment.producer.PaymentResultProducer;
import com.example.payment.service.PaymentProcessingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeatConfirmedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentProcessingService paymentProcessingService;
    private final PaymentResultProducer paymentResultProducer;

    @KafkaListener(topics = "seat-confirmed-events", groupId = "payment-group")
    public void handleSeatConfirmed(ConsumerRecord<String, String> record) {
        // Trich xuat correlationId tu HEADER
        String correlationId = extractCorrelationId(record);

        log.info("[PaymentService] Processing Payment for {}. CorrelationID: {}",
                record.key(), correlationId);

        SeatAllocationEvent event = objectMapper.readValue(record.value(), SeatAllocationEvent.class);
        PaymentEvent paymentResult = paymentProcessingService.processPayment(event);

        log.info("[PaymentService] Payment success: {} VND. CorrelationID: {}",
                paymentResult.getTotalPrice(), correlationId);

        paymentResultProducer.publishPaymentResult(paymentResult, correlationId);
    }

    private String extractCorrelationId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("correlationId");
        if (header == null) {
            throw new IllegalStateException("Thieu correlationId trong header cua su kien seat-confirmed-events");
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
