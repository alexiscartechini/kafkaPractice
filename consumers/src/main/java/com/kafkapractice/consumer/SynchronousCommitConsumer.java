package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class SynchronousCommitConsumer extends AbstractKafkaConsumer<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(SynchronousCommitConsumer.class);
    private static final String TEST_TOPIC = "test-topic";

    public SynchronousCommitConsumer(Map<String, Object> consumerProperties) {
        super(consumerProperties);
    }

    public static void main(String[] args) {
        SynchronousCommitConsumer messageConsumer = new SynchronousCommitConsumer(baseConsumerProperties());
        messageConsumer.pollKafka();
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
                if (consumerRecords.count() > 0) {
                    kafkaConsumer.commitSync();
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