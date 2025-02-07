package com.kafka.ms.email.handler;

import com.kafka.ms.email.exception.NotRetryableException;
import com.kafka.ms.email.exception.RetryableException;
import com.kafka.ms.events.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/*
  * we can use groupId in @KafkaListener to set consumer group when we have more than one instances
  * or we can configure in @configuration class ConsumerFactory bean e.g:config.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

//@KafkaListener(topics="product-created-events-topic",groupId = "product-created-events-group")
*/
@Component
//@KafkaListener(topics = {"topic1" , "topic2"}) one consumer can listener to multiple topics
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

        //Add a logic if price is less than 0 stop retry message and publish to DLT
        //This NotRetryableException.class is added in DefaultErrorHandler in KafkaConsumerConfiguration
        //as long as NotRetryableException throws DefaultErrorHandler will publish DLT automatically
        if(productCreatedEvent.getPrice() < 0){
            logger.error("Price can't be less than 0 not point to retry the message. Publish message to DLT");
            throw new NotRetryableException("Price can't be less than 0 not point to retry the message. Publish message to DLT");
        }
        logger.info("Retry the message for testing retryable exception");
        throw new RetryableException("Retry the message for testing retryable exception");

    }
}
