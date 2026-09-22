package com.example.seat.service;

import org.springframework.stereotype.Service;

import com.example.seat.model.CinemaBookingRequest;

@Service
public class SeatAllocationService {

    public String reserveSeats(CinemaBookingRequest request) {
        // Mo phong nghiep vu giu ghe: coi nhu luon thanh cong
        return String.join(", ", request.getSeatNumbers());
    }
}
