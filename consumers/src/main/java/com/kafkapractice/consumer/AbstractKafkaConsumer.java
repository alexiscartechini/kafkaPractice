package com.kafkapractice.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class AbstractKafkaConsumer<K, V> {

    protected final KafkaConsumer<K, V> kafkaConsumer;
    protected volatile boolean running = true;

    public AbstractKafkaConsumer(Map<String, Object> properties) {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }
}