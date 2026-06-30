package edu.eci.arsw.kafka.consumer;

import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void consumeForNotification(PaymentProcessedEvent event) {
        String message = "APPROVED".equals(event.getStatus())
            ? "Pago aprobado para orden " + event.getOrderId()
            : "Pago rechazado para orden " + event.getOrderId();
        log.info("[notification-service] {}", message);
    }

    @KafkaListener(topics = "payments", groupId = "audit-service")
    public void consumeForAudit(PaymentProcessedEvent event) {
        log.info("[audit-service] Pago auditado - paymentId: {}, orderId: {}, status: {}",
            event.getPaymentId(), event.getOrderId(), event.getStatus());
    }
}
