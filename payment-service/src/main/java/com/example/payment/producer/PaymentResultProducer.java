package com.example.payment.producer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.payment.model.PaymentEvent;

import lombok.RequiredArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class PaymentResultProducer {

    private static final String TOPIC = "payment-result-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishPaymentResult(PaymentEvent event, String correlationId) {
        String payload = objectMapper.writeValueAsString(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, event.getCinemaBookingId(), payload);

        // Ket thuc chuoi su kien nhung van giu nguyen correlationId trong header
        record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record);
    }
}
