package com.example.kafka.shipping.ares.resilience;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Service
public class LocalDockerOperationsPort implements DockerOperationsPort {

    @Override
    public String execute(String experimentType, String affectedService, int durationSeconds) {
        String normalizedType = experimentType == null ? "UNKNOWN" : experimentType.trim().toUpperCase();
        String target = affectedService == null || affectedService.isBlank() ? "unknown-service" : affectedService;

        if (!isDockerAvailable()) {
            return "Docker CLI is unavailable in this environment; the experiment was recorded but not executed.";
        }

        return switch (normalizedType) {
            case "STOP_CONTAINER" -> runDockerCommand("stop", target);
            case "RESTART_SERVICE" -> runDockerCommand("restart", target);
            case "INJECT_LATENCY" -> "Simulated latency injection for " + target + " for " + durationSeconds + " seconds.";
            case "SIMULATE_PACKET_LOSS" -> "Simulated packet loss for " + target + " for " + durationSeconds + " seconds.";
            case "SIMULATE_KAFKA_FAILURE" -> "Simulated Kafka outage for " + target + " for " + durationSeconds + " seconds.";
            case "SIMULATE_DATABASE_FAILURE" -> "Simulated database outage for " + target + " for " + durationSeconds + " seconds.";
            default -> "Unsupported experiment type: " + experimentType;
        };
    }

    private boolean isDockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "version").start();
            return process.waitFor() == 0;
        } catch (Exception ex) {
            return false;
        }
    }

    private String runDockerCommand(String action, String target) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add(action);
        command.add(target);

        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = readOutput(process);
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return "Docker " + action + " executed for " + target + ".";
            }
            return "Docker " + action + " failed for " + target + ": " + output;
        } catch (Exception ex) {
            return "Docker " + action + " failed for " + target + ": " + ex.getMessage();
        }
    }

    private String readOutput(Process process) throws Exception {
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }
        return output.toString().trim();
    }
}
