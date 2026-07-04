package com.example.kafka.shipping.ares.resilience;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/ares/resilience")
public class ResilienceController {

    private final ResilienceEnginePort resilienceEnginePort;

    public ResilienceController(ResilienceEnginePort resilienceEnginePort) {
        this.resilienceEnginePort = resilienceEnginePort;
    }

    @PostMapping("/run")
    public ResponseEntity<ExperimentExecutionResponse> run(@RequestBody ResilienceExperimentRequest request) {
        return ResponseEntity.ok(resilienceEnginePort.runExperiment(request));
    }

    @PostMapping("/stop")
    public ResponseEntity<ExperimentExecutionResponse> stop(@RequestBody ResilienceStopRequest request) {
        return ResponseEntity.ok(resilienceEnginePort.stopExperiment(request));
    }

    @GetMapping("/history")
    public ResponseEntity<List<ExperimentExecutionResponse>> history() {
        return ResponseEntity.ok(resilienceEnginePort.getHistory());
    }
}
