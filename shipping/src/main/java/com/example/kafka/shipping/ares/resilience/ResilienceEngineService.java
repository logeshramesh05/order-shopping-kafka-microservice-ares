package com.example.kafka.shipping.ares.resilience;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class ResilienceEngineService implements ResilienceEnginePort {

    private final ResilienceExperimentRepository resilienceExperimentRepository;
    private final DockerOperationsPort dockerOperationsPort;

    public ResilienceEngineService(ResilienceExperimentRepository resilienceExperimentRepository,
                                   DockerOperationsPort dockerOperationsPort) {
        this.resilienceExperimentRepository = resilienceExperimentRepository;
        this.dockerOperationsPort = dockerOperationsPort;
    }

    @Override
    public ExperimentExecutionResponse runExperiment(ResilienceExperimentRequest request) {
        ResilienceExperimentRecord record = new ResilienceExperimentRecord();
        record.setName(request.getName());
        record.setExperimentType(normalize(request.getExperimentType()));
        record.setDurationSeconds(request.getDurationSeconds() == null ? 30 : request.getDurationSeconds());
        record.setAffectedService(request.getAffectedService());
        record.setStatus(ExperimentStatus.RUNNING);
        record.setTimestamp(Instant.now());
        record.setDetails(request.getNotes());
        record = resilienceExperimentRepository.save(record);

        Integer durationSeconds = record.getDurationSeconds();
        if (durationSeconds == null) {
            durationSeconds = request.getDurationSeconds() == null ? 30 : request.getDurationSeconds();
        }

        String experimentType = record.getExperimentType();
        if (experimentType == null) {
            experimentType = normalize(request.getExperimentType());
        }

        String affectedService = record.getAffectedService();
        if (affectedService == null) {
            affectedService = request.getAffectedService();
        }

        String outcome = dockerOperationsPort.execute(experimentType, affectedService, durationSeconds);
        record.setResultSummary(outcome);
        record.setStatus(isFailure(outcome) ? ExperimentStatus.FAILED : ExperimentStatus.COMPLETED);
        record = resilienceExperimentRepository.save(record);

        return ExperimentExecutionResponse.fromRecord(record);
    }

    @Override
    public ExperimentExecutionResponse stopExperiment(ResilienceStopRequest request) {
        ResilienceExperimentRecord record = new ResilienceExperimentRecord();
        record.setName("Stop experiment");
        record.setExperimentType("STOP_CONTAINER");
        record.setDurationSeconds(request.getDurationSeconds() == null ? 0 : request.getDurationSeconds());
        record.setAffectedService(request.getAffectedService());
        record.setStatus(ExperimentStatus.RUNNING);
        record.setTimestamp(Instant.now());
        record.setDetails(request.getReason());
        record = resilienceExperimentRepository.save(record);

        Integer durationSeconds = record.getDurationSeconds();
        if (durationSeconds == null) {
            durationSeconds = request.getDurationSeconds() == null ? 0 : request.getDurationSeconds();
        }

        String experimentType = record.getExperimentType();
        if (experimentType == null) {
            experimentType = "STOP_CONTAINER";
        }

        String affectedService = record.getAffectedService();
        if (affectedService == null) {
            affectedService = request.getAffectedService();
        }

        String outcome = dockerOperationsPort.execute(experimentType, affectedService, durationSeconds);
        record.setResultSummary(outcome);
        record.setStatus(isFailure(outcome) ? ExperimentStatus.FAILED : ExperimentStatus.STOPPED);
        record = resilienceExperimentRepository.save(record);

        return ExperimentExecutionResponse.fromRecord(record);
    }

    @Override
    public List<ExperimentExecutionResponse> getHistory() {
        return resilienceExperimentRepository.findAllByOrderByTimestampDesc().stream()
                .map(ExperimentExecutionResponse::fromRecord)
                .toList();
    }

    private String normalize(String experimentType) {
        return experimentType == null ? "UNKNOWN" : experimentType.trim().toUpperCase();
    }

    private boolean isFailure(String outcome) {
        String normalized = outcome == null ? "" : outcome.toLowerCase();
        return normalized.contains("failed") || normalized.contains("unavailable") || normalized.contains("unsupported");
    }
}
