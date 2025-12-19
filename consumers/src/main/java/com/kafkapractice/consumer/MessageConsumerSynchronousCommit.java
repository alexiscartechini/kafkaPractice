package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerSynchronousCommit {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerSynchronousCommit.class);
    private static final String TEST_TOPIC = "test-topic";
    private final KafkaConsumer<String, String> kafkaConsumer;
    private volatile boolean running = true;

    public static void main(String[] args) {
        MessageConsumerSynchronousCommit messageConsumer = new MessageConsumerSynchronousCommit(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public MessageConsumerSynchronousCommit(Map<String, Object> consumerProperties){
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    public static  Map<String, Object> buildConsumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092,localhost:29093,localhost:29094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup2");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public void pollKafka(){
        kafkaConsumer.subscribe(List.of(TEST_TOPIC));

        try {
            while (running){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord ->
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            consumerRecord.key(), consumerRecord.value(), consumerRecord.partition())
                );
                if (consumerRecords.count()>0){
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
