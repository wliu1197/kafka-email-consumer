package com.kafka.ms.email.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
@Configuration
public class KafkaConsumerConfiguration {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Value("${kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.consumer.key-deserializer}")
    private String keyDeserializer;
    @Value("${kafka.consumer.value-deserializer}")
    private String valueDeserializer;
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    @Value("${kafka.consumer.properties.spring.json.trusted.packages}")
    private String trustedPackage;

    @Bean
    public ConsumerFactory<String,Object> consumerFactory(){
        Map<String,Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,keyDeserializer);
       // config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,valueDeserializer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        config.put(JsonDeserializer.TRUSTED_PACKAGES,trustedPackage);
        // to Handler Deserializer error using bean config
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,JsonDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(config);
    }
    //configurations for producerFactory
    @Bean
    ProducerFactory<String, Object> producerFactory(){
        Map<String,Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerConfigs.put(ProducerConfig.RETRIES_CONFIG,10);
        producerConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,1000);
        producerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,29000);
        producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG,0);
        producerConfigs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,29000);
        return new DefaultKafkaProducerFactory<>(producerConfigs);
    }
    //Creates KafkaTemplate to send message to Dead Letter Topic
    @Bean
    KafkaTemplate<String,Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,Object> kafkaListenerContainerFactory(
                    ConsumerFactory<String,Object> consumerFactory,
                    KafkaTemplate<String,Object> kafkaTemplate){
        //configure
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate));
        ConcurrentKafkaListenerContainerFactory<String,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //consumer factory
        factory.setConsumerFactory(consumerFactory);
        //error handler deserialization error to Dead letter topic product-created-events-topic.DLT
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
    @Bean
    NewTopic createTopic(){
        NewTopic topic =  TopicBuilder.name("product-created-events-topic-dlt")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas","1"))
                .build();
        logger.info("-----------------product-created-events-topic-dlt created ---------------");
        return topic;
    }

}
