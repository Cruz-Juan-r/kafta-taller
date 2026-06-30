package edu.eci.arsw.kafka.producer;

import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String TOPIC = "payments";

    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(PaymentProcessedEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
        log.info("Evento publicado en topic '{}' con clave '{}': {}", TOPIC, event.getOrderId(), event);
    }
}
