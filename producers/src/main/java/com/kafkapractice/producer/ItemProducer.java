package com.kafkapractice.producer;

import com.kafkapractice.domain.Item;
import com.kafkapractice.serializer.ItemSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.nonNull;

public class ItemProducer {

    private static final Logger logger = LoggerFactory.getLogger(ItemProducer.class);

    String topicName = "test-topic";
    KafkaProducer<Integer, Item> kafkaProducer;

    public ItemProducer(Map<String, Object> producerProperties){
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

    public static Map<String, Object> buildProducerProperties(){
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        propertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());
        propertiesMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propertiesMap.put(ProducerConfig.RETRIES_CONFIG, 30);
        propertiesMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        return propertiesMap;
    }

    public void publishMessageSynchronously(Item item){
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord<>(topicName, item.getId(), item);
        try {
            kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent successfully for the key {}", item, item.getId());
        } catch (InterruptedException|ExecutionException e) {
            logger.error("Exception in publishMessageSynchronously: {}", e.getMessage());
        }
    }

    public void publishMessageAsynchronously(Item item){
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord<>(topicName, item.getId(), item);
        try {
            kafkaProducer.send(producerRecord, callback).get();
            logger.info("Message {} sent successfully for the key {}", item, item.getId());
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