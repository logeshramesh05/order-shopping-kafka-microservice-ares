package com.example.kafka.shipping;


import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name="shipping")
public class Shipping {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long Id;

    private String customerName;

    private double totalCost;

    private String address;

    private boolean shipping;

    public Shipping(String customerName, double totalCost, String address, boolean shipping) {
        this.customerName = customerName;
        this.totalCost = totalCost;
        this.address = address;
        this.shipping = shipping;
    }
}
