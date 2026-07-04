package com.example.kafka.shipping.ares.resilience;

import lombok.Data;

@Data
public class ResilienceExperimentRequest {
    private String name;
    private String experimentType;
    private Integer durationSeconds;
    private String affectedService;
    private String notes;
}
