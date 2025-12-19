package com.kafkapractice.consumer;

import com.kafkapractice.deserializer.ItemDeserializer;
import com.kafkapractice.domain.Item;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ItemConsumer.class);
    private static final String TEST_TOPIC = "test-topic";
    private final KafkaConsumer<Integer, Item> kafkaConsumer;
    private volatile boolean running = true;

    public ItemConsumer(Map<String, Object> consumerProperties) {
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    public static void main(String[] args) {
        ItemConsumer messageConsumer = new ItemConsumer(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:29093,localhost:29094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ItemDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup2");
        return properties;
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TEST_TOPIC));

        try {
            while (running) {
                ConsumerRecords<Integer, Item> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord -> logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.partition()));
            }
        } catch (Exception e) {
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
