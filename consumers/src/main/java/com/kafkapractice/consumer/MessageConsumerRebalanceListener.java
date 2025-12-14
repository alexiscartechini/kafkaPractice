package com.kafkapractice.consumer;

import com.kafkapractice.listener.MessageRebalanceListener;
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

public class MessageConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerRebalanceListener.class);
    private KafkaConsumer<String, String> kafkaConsumer;
    private String topicName = "test-topic";

    public static void main(String[] args) {
        MessageConsumerRebalanceListener messageConsumer = new MessageConsumerRebalanceListener(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public MessageConsumerRebalanceListener(Map<String, Object> consumerProperties){
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    public static  Map<String, Object> buildConsumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092,localhost:29093,localhost:29094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup2");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        return properties;
    }

    public void pollKafka(){
        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener());

        try {
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((record) -> {
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            record.key(), record.value(), record.partition());
                });
            }
        } catch (Exception e){
            logger.error("Exception in poll(): ", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
