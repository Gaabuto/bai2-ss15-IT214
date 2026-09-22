package com.example.payment.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    private String cinemaBookingId;
    private String transactionId;
    private long totalPrice;
    private String paymentStatus;
}
