package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class MessageConsumer {

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic";

    public MessageConsumer(Map<String, Object> getConsumerProperties){
        kafkaConsumer = new KafkaConsumer<String, String>(getConsumerProperties);
    }

    public static  Map<String, Object> buildConsumerProperties(){

    }
}
