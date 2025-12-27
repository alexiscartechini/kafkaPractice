package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionCommitConsumer extends AbstractKafkaConsumer<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(PartitionCommitConsumer.class);
    private static final String TEST_TOPIC = "test-topic";
    private final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    public PartitionCommitConsumer(Map<String, Object> consumerProperties) {
        super(consumerProperties);
    }

    public static void main(String[] args) {
        PartitionCommitConsumer messageConsumer = new PartitionCommitConsumer(baseConsumerProperties());
        messageConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TEST_TOPIC));

        try {
            while (running) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord -> {
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
                    offsetMap.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1, null));
                });
                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync(offsetMap);
                    logger.info("COMMIT");
                }
            }
        } catch (CommitFailedException e) {
            logger.error("Commit exception: ", e);
        } catch (Exception e) {
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}