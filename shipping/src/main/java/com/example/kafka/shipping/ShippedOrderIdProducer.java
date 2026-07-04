package com.example.kafka.shipping;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ShippedOrderIdProducer {

    private final KafkaTemplate<String, Long> kafkaTemplate;

    public ShippedOrderIdProducer(KafkaTemplate<String, Long> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceOrderId(Long orderId) {
        kafkaTemplate.send(AppConstants.SHIPPED_ORDER_TOPIC, orderId);
    }
}
