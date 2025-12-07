package com.kafkapractice.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    String topicName = "test-topic";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propertiesMap){
        kafkaProducer = new KafkaProducer<>(propertiesMap);
    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(getMappingProperties());
        messageProducer.publishMessageSynchronously(null, "ABC");
    }

    public static Map<String, Object> getMappingProperties(){
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        return propertiesMap;
    }

    public void publishMessageSynchronously(String key, String value){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            System.out.println("Partition: " + recordMetadata.partition() + " - Offset: " + recordMetadata.offset());
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeException(e);
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}