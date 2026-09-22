package com.example.movie.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.movie.model.CinemaBookingRequest;
import com.example.movie.service.BookingPublisherService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/bookings")
@RequiredArgsConstructor
public class MovieBookingController {

    private final BookingPublisherService bookingPublisherService;

    @PostMapping
    public ResponseEntity<Map<String, Object>> createBooking(@RequestBody CinemaBookingRequest request) {
        String correlationId = bookingPublisherService.createBooking(request);

        return ResponseEntity.accepted().body(Map.of(
                "cinemaBookingId", request.getCinemaBookingId(),
                "correlationId", correlationId,
                "status", "BOOKING_CREATED"
        ));
    }
}
