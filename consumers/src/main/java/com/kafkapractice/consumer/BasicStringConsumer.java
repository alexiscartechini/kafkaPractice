package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class BasicStringConsumer extends AbstractKafkaConsumer<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(BasicStringConsumer.class);
    private static final String TEST_TOPIC = "test-topic";

    public BasicStringConsumer(Map<String, Object> consumerProperties) {
        super(consumerProperties);
    }

    public static void main(String[] args) {
        BasicStringConsumer basicStringConsumer = new BasicStringConsumer(baseConsumerProperties());
        basicStringConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TEST_TOPIC));

        try {
            while (running) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord ->
                        logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                                consumerRecord.key(), consumerRecord.value(), consumerRecord.partition())
                );
            }
        } catch (Exception e) {
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}