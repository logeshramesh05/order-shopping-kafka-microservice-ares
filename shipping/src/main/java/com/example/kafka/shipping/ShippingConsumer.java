package com.example.kafka.shipping;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;

@Service
public class ShippingConsumer {

    @Autowired
    private ShippingController shippingController;



    @KafkaListener(topics = AppConstants.ORDER_TOPIC)
    public void consumeOrder(String shippingOrderString) throws IOException {
        System.out.println("Consumed Order -> "+shippingOrderString);

        ObjectMapper mapper = new ObjectMapper();

        Shipping shippingOrder = mapper.readValue(shippingOrderString,Shipping.class);
        shippingController.saveShipping(shippingOrder);
    }



}
