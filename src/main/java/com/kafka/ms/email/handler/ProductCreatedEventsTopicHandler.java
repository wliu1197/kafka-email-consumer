package com.kafka.ms.email.handler;

import com.kafka.ms.email.exception.NotRetryableException;
import com.kafka.ms.email.exception.RetryableException;
import com.kafka.ms.email.model.ProcessedEventDetails;
import com.kafka.ms.email.service.ProcessedEventAgent;
import com.kafka.ms.email.service.ProcessedEventImpl;
import com.kafka.ms.events.ProductCreatedEvent;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

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
    private final boolean throwRetryableExe = false;

    private ProcessedEventAgent processedEventAgent;
    @Autowired
    public ProductCreatedEventsTopicHandler(@Qualifier("ProcessedEventService") ProcessedEventAgent processedEventAgent) {
        this.processedEventAgent = processedEventAgent;
    }
    // @Transactional apply DB transaction to this method
    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header(value = "messageId",required = false) String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey){
        logger.info("-------------------- Handling ProductCreatedEvent ---------------------------");
        logger.info("Received a new event messageId:" +  messageId);
        logger.info("Received a new event messageKey:" + messageKey);
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

        ProcessedEventDetails processedEvents = processedEventAgent.findByMessageId(messageId);
        // message not in db then store it
        if(processedEvents == null){
            ProcessedEventDetails processedEventDetails = new ProcessedEventDetails();
            processedEventDetails.setProductId(productCreatedEvent.getProductId());
            processedEventDetails.setMessageId(messageId);
            processedEventDetails.setEvent(productCreatedEvent.toString());
            processedEventAgent.saveEvent(processedEventDetails);
            //todo: processing event logic here...do something...maybe send email and then producer message to a different topic
        }else {
            //if message has already processed then do nothing move message to DLT so it won't process again;
            logger.info("Message has already been processed. Message Id:" + messageId + "publish it to DLT");
            throw new NotRetryableException("Message has already been processed. Message Id:" + messageId + "publish it to DLT");
        }

        if(throwRetryableExe) {
            logger.info("Retry the message for testing retryable exception");
            throw new RetryableException("Retry the message for testing retryable exception");
        }
    }
}
