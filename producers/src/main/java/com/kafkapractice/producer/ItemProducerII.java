package com.kafkapractice.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkapractice.domain.Item;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ItemProducerII {

    private static final Logger logger = LoggerFactory.getLogger(ItemProducerII.class);

    private static final String TEST_TOPIC = "test-topic";
    private final KafkaProducer<Integer, String> kafkaProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ItemProducerII(Map<String, Object> producerProperties) {
        kafkaProducer = new KafkaProducer<>(producerProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        ItemProducerII messageProducer = new ItemProducerII(buildProducerProperties());

        Item item = new Item(1, "Chimuelo teddy bear", 52.30);
        Item item1 = new Item(2, "Pikachu teddy bear", 44.71);

        try {
            messageProducer.publishMessageSynchronously(item);
            messageProducer.publishMessageSynchronously(item1);
        } catch (JsonProcessingException e) {
            logger.error("Error publishing messages: {}", e.getMessage());
        }

        Thread.sleep(3000);
    }

    public static Map<String, Object> buildProducerProperties() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesMap.put(ProducerConfig.RETRIES_CONFIG, 30);
        propertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propertiesMap;
    }

    public void publishMessageSynchronously(Item item) throws JsonProcessingException {
        String itemToString = objectMapper.writeValueAsString(item);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, item.getId(), itemToString);
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