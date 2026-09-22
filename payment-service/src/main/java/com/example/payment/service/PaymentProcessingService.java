package com.example.payment.service;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.example.payment.model.PaymentEvent;
import com.example.payment.model.SeatAllocationEvent;

@Service
public class PaymentProcessingService {

    public PaymentEvent processPayment(SeatAllocationEvent event) {
        // Mo phong nghiep vu thanh toan: coi nhu luon thanh cong
        String transactionId = UUID.randomUUID().toString();
        return new PaymentEvent(
                event.getCinemaBookingId(),
                transactionId,
                event.getTotalPrice(),
                "PAYMENT_SUCCESS"
        );
    }
}
