package com.example.kafka.shipping.ares.resilience;

import java.util.List;

public interface ResilienceEnginePort {
    ExperimentExecutionResponse runExperiment(ResilienceExperimentRequest request);
    ExperimentExecutionResponse stopExperiment(ResilienceStopRequest request);
    List<ExperimentExecutionResponse> getHistory();
}
