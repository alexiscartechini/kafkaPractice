package com.kafkapractice.producer;

import com.kafkapractice.domain.Item;
import com.kafkapractice.serializer.ItemSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ItemProducer {

    private static final Logger logger = LoggerFactory.getLogger(ItemProducer.class);

    String topicName = "test-topic";
    KafkaProducer<Integer, Item> kafkaProducer;

    public ItemProducer(Map<String, Object> producerProperties) {
        kafkaProducer = new KafkaProducer<>(producerProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        ItemProducer messageProducer = new ItemProducer(buildProducerProperties());
        Item item = new Item(1, "Chimuelo teddy bear", 52.30);
        Item item1 = new Item(2, "Pikachu teddy bear", 44.71);

        messageProducer.publishMessageSynchronously(item);
        messageProducer.publishMessageSynchronously(item1);

        Thread.sleep(3000);
    }

    public static Map<String, Object> buildProducerProperties() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());
        propertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesMap.put(ProducerConfig.RETRIES_CONFIG, 30);
        propertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propertiesMap;
    }

    public void publishMessageSynchronously(Item item) {
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord<>(topicName, item.getId(), item);
        try {
            kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent successfully for the key {}", item, item.getId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Thread interrupted while sending message", e);
        } catch (ExecutionException e) {
            logger.error("Error sending message to Kafka", e);
        }
    }
}