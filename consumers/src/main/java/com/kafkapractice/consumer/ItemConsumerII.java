package com.kafkapractice.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkapractice.domain.Item;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemConsumerII {

    private static final Logger logger = LoggerFactory.getLogger(ItemConsumerII.class);
    private KafkaConsumer<Integer, String> kafkaConsumer;
    private String topicName = "test-topic";
    private ObjectMapper objectMaper = new ObjectMapper();

    public static void main(String[] args) {
        ItemConsumerII messageConsumer = new ItemConsumerII(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public ItemConsumerII(Map<String, Object> consumerProperties){
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    public static  Map<String, Object> buildConsumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092,localhost:29093,localhost:29094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup2");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public void pollKafka(){
        kafkaConsumer.subscribe(List.of(topicName));

        try {
            while (true){
                ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((record) -> {
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            record.key(), record.value(), record.partition());
                    try {
                        Item item = objectMaper.readValue(record.value(), Item.class);
                        logger.info("Item: {}", item);
                    } catch (JsonProcessingException e) {
                        logger.error("Error deserializing: {}", e.getMessage());
                    }
                });
            }
        } catch (Exception e){
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
