package edu.eci.arsw.kafka.dto;

import java.time.Instant;

public class PaymentProcessedEvent {

    private String paymentId;
    private String orderId;
    private String customerId;
    private Double total;
    private String status;
    private Instant occurredAt;

    public PaymentProcessedEvent() {}

    public PaymentProcessedEvent(String paymentId, String orderId, String customerId,
                                  Double total, String status, Instant occurredAt) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.total = total;
        this.status = status;
        this.occurredAt = occurredAt;
    }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public Double getTotal() { return total; }
    public void setTotal(Double total) { this.total = total; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    @Override
    public String toString() {
        return "PaymentProcessedEvent{paymentId='" + paymentId + "', orderId='" + orderId +
               "', status='" + status + "', total=" + total + "}";
    }
}
