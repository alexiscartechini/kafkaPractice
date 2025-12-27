package com.kafkapractice.consumer;

import com.kafkapractice.listener.OffsetRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class RebalanceAwareConsumer extends AbstractKafkaConsumer<String, String>{

    private static final Logger logger = LoggerFactory.getLogger(RebalanceAwareConsumer.class);
    private static final String TEST_TOPIC = "test-topic";

    public RebalanceAwareConsumer(Map<String, Object> consumerProperties) {
        super(consumerProperties);
    }

    public static void main(String[] args) {
        RebalanceAwareConsumer messageConsumer = new RebalanceAwareConsumer(baseConsumerProperties());
        messageConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TEST_TOPIC), new OffsetRebalanceListener(kafkaConsumer));

        try {
            while (running) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord ->
                        logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                                consumerRecord.key(), consumerRecord.value(), consumerRecord.partition())
                );
                kafkaConsumer.commitSync();
            }
        } catch (Exception e) {
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}