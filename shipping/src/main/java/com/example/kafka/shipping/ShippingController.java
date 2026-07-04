package com.example.kafka.shipping;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping("/api/shipping")
public class ShippingController {

    @Autowired
    private ShippingRepository shippingRepository;

    private final ShippedOrderIdProducer shippedOrderIdProducer;

    public ShippingController(ShippedOrderIdProducer shippedOrderIdProducer) {
        this.shippedOrderIdProducer = shippedOrderIdProducer;
    }

    @DeleteMapping("/{orderId}")
    public String saveAndProduceOrderId(@PathVariable Long orderId) {
        if (!shippingRepository.existsById(orderId)) {
            return "Order id " + orderId + " not found in shipping database.";
        }

        shippedOrderIdProducer.produceOrderId(orderId);
        shippingRepository.deleteById(orderId);

        return "Order " + orderId + " is shipped (deleted) from shipping database and produced to Kafka.";
    }

    @GetMapping
    public List<Shipping> getOrderToShipping() {
        return shippingRepository.findAll();
    }




    public void saveShipping(Shipping shippingOrder) {
        shippingRepository.save(shippingOrder);

        System.out.println("saved in shipping db");
    }





}
