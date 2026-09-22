package com.example.payment.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeatAllocationEvent {

    private String cinemaBookingId;
    private String movieCode;
    private List<String> seatNumbers;
    private String customerEmail;
    private long totalPrice;
    private String seatAllocationStatus;
}
