package com.kafka.ms.email.handler;

import com.kafka.ms.events.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventsTopicHandler {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        logger.info("-------------------- Handling ProductCreatedEvent ---------------------------");
        logger.info("Received a new event productid:" + productCreatedEvent.getProductId());
        logger.info("Received a new event title:" + productCreatedEvent.getTitle());
        logger.info("Received a new event price:" + productCreatedEvent.getPrice());
        logger.info("Received a new event quantity:" + productCreatedEvent.getQuantity());
    }
}
