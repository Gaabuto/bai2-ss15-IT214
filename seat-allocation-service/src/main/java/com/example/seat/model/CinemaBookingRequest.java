package com.example.seat.model;

import java.time.LocalDateTime;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CinemaBookingRequest {

    private String cinemaBookingId;
    private String movieCode;
    private LocalDateTime showTime;
    private List<String> seatNumbers;
    private String customerEmail;
    private long totalPrice;
}
