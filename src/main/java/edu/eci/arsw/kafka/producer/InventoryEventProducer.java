package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String TOPIC = "inventory";

    private final KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, InventoryProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(InventoryProcessedEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
        log.info("Evento publicado en topic '{}' con clave '{}': {}", TOPIC, event.getOrderId(), event);
    }
}
