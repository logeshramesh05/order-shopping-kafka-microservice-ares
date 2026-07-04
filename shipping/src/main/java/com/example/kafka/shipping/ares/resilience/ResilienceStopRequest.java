package com.example.kafka.shipping.ares.resilience;

import lombok.Data;

@Data
public class ResilienceStopRequest {
    private String affectedService;
    private String reason;
    private Integer durationSeconds;
}
