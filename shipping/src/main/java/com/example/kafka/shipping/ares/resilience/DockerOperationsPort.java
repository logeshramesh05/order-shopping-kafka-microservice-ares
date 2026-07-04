package com.example.kafka.shipping.ares.resilience;

public interface DockerOperationsPort {
    String execute(String experimentType, String affectedService, int durationSeconds);
}
