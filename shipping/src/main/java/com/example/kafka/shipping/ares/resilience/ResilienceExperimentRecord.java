package com.example.kafka.shipping.ares.resilience;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "ares_resilience_experiments")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResilienceExperimentRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private String experimentType;

    private Integer durationSeconds;

    private String affectedService;

    @Enumerated(EnumType.STRING)
    private ExperimentStatus status;

    private Instant timestamp;

    private String resultSummary;

    private String details;
}
