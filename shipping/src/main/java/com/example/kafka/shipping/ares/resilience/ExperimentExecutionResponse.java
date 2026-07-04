package com.example.kafka.shipping.ares.resilience;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExperimentExecutionResponse {
    private Long id;
    private String name;
    private String experimentType;
    private String status;
    private String affectedService;
    private Instant timestamp;
    private Integer durationSeconds;
    private String resultSummary;
    private String details;

    public static ExperimentExecutionResponse fromRecord(ResilienceExperimentRecord record) {
        return new ExperimentExecutionResponse(
                record.getId(),
                record.getName(),
                record.getExperimentType(),
                record.getStatus() == null ? null : record.getStatus().name(),
                record.getAffectedService(),
                record.getTimestamp(),
                record.getDurationSeconds(),
                record.getResultSummary(),
                record.getDetails()
        );
    }
}
