package com.kafkapractice.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.nonNull;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> producerProperties){
        kafkaProducer = new KafkaProducer<>(producerProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        MessageProducer messageProducer = new MessageProducer(buildProducerProperties());
        messageProducer.publishMessageSynchronously("1", "ABC");
        messageProducer.publishMessageSynchronously("1", "DEF");
        Thread.sleep(3000);
    }

    public static Map<String, Object> buildProducerProperties(){
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesMap.put(ProducerConfig.RETRIES_CONFIG, 30);
        propertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propertiesMap;
    }

    public void publishMessageSynchronously(String key, String value){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        try {
            kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent successfully for the key {}", value, key);
        } catch (InterruptedException|ExecutionException e) {
            logger.error("Exception in publishMessageSynchronously: {}", e.getMessage());
        }
    }

    public void publishMessageAsynchronously(String key, String value){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        try {
            kafkaProducer.send(producerRecord, callback).get();
            logger.info("Message {} sent successfully for the key {}", value, key);
        } catch (InterruptedException|ExecutionException e) {
            logger.error("Exception in publishMessageSynchronously: {}", e.getMessage());
        }
    }

    Callback callback = (metadata, exception) -> {
        if(nonNull(exception)){
            logger.error("Exception in callback {}", exception.getMessage());
        } else {
            logger.info("Published message offset in callback is {} and the partition is {}",
                    metadata.offset(), metadata.partition());
        }
    };

    public void close(){
        kafkaProducer.close();
    }
}