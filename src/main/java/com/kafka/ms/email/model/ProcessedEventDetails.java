package com.kafka.ms.email.model;

import jakarta.persistence.*;
import org.springframework.boot.autoconfigure.web.WebProperties;

@Entity(name="processed_event")
public class ProcessedEventDetails {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    @Column(name="product_id")
    String productId;
    @Column(name="message_id")
    String messageId;
    @Column(name="event")
    String event;

    public ProcessedEventDetails() {
        super();
    }

    public ProcessedEventDetails(Integer id, String productId, String messageId, String event) {
        this.id = id;
        this.productId = productId;
        this.messageId = messageId;
        this.event = event;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}
