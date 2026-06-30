package edu.eci.arsw.kafka.controller;

import edu.eci.arsw.kafka.dto.CreateOrderRequest;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.OrderEventProducer;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderCreatedEvent createOrder(@RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = new OrderCreatedEvent(
            "ORD-" + UUID.randomUUID(),
            request.getCustomerId(),
            request.getTotal(),
            "CREATED",
            Instant.now()
        );
        producer.publishOrderCreated(event);
        return event;
    }
}
