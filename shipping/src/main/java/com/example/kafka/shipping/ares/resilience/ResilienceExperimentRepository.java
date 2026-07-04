package com.example.kafka.shipping.ares.resilience;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ResilienceExperimentRepository extends JpaRepository<ResilienceExperimentRecord, Long> {
    List<ResilienceExperimentRecord> findAllByOrderByTimestampDesc();
}
