package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import edu.eci.arsw.kafka.producer.InventoryEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final PaymentEventProducer paymentProducer;
    private final InventoryEventProducer inventoryProducer;

    public OrderEventConsumer(PaymentEventProducer paymentProducer,
                               InventoryEventProducer inventoryProducer) {
        this.paymentProducer = paymentProducer;
        this.inventoryProducer = inventoryProducer;
    }

    // Consumidor del grupo inventory-service (Actividad 4)
    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void consumeForInventory(OrderCreatedEvent event) {
        log.info("[inventory-service] Evento recibido: {}", event.getOrderId());

        boolean reserved = event.getTotal() <= 300000;
        InventoryProcessedEvent inventoryEvent = new InventoryProcessedEvent(
            "INV-" + UUID.randomUUID(),
            event.getOrderId(),
            event.getCustomerId(),
            reserved ? "RESERVED" : "REJECTED",
            Instant.now()
        );
        inventoryProducer.publish(inventoryEvent);
    }

    // Consumidor del grupo payment-service
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consumeForPayment(OrderCreatedEvent event) {
        log.info("[payment-service] Evento recibido: {}", event.getOrderId());

        boolean approved = event.getTotal() <= 250000;
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
            "PAY-" + UUID.randomUUID(),
            event.getOrderId(),
            event.getCustomerId(),
            event.getTotal(),
            approved ? "APPROVED" : "REJECTED",
            Instant.now()
        );
        paymentProducer.publish(paymentEvent);
    }

    // Consumidor del grupo analytics-service
    @KafkaListener(topics = "orders", groupId = "analytics-service")
    public void consumeForAnalytics(OrderCreatedEvent event) {
        log.info("[analytics-service] Registrando métricas para orden: {} - total: {}", event.getOrderId(), event.getTotal());
    }

    // Consumidor del grupo audit-service
    @KafkaListener(topics = "orders", groupId = "audit-service")
    public void consumeForAudit(OrderCreatedEvent event) {
        log.info("[audit-service] Auditoría registrada - orderId: {}, customerId: {}, total: {}, timestamp: {}",
            event.getOrderId(), event.getCustomerId(), event.getTotal(), event.getOccurredAt());
    }
}
