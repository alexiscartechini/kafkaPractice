package com.kafkapractice.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MessageRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageRebalanceListener.class);
    public static final String FILE_PATH = "consumers/src/main/resources/offset.ser";
    private final KafkaConsumer<String, String> kafkaConsumer;

    public MessageRebalanceListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.info("onPartitionsRevoked: {}", collection);
        kafkaConsumer.commitSync();
        logger.info("offsets committed");

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.info("onPartitionsAssigned: {}", collection);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = readOffsetSerializationFile();
        logger.info("OffsetMap: {}", offsetMap);
        if(!offsetMap.isEmpty()){
            collection.forEach(partition ->
                kafkaConsumer.seek(partition, offsetMap.get(partition))
            );
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile(){
        try (FileInputStream fileInputStream = new FileInputStream(FILE_PATH);
             BufferedInputStream bufferedInputStream  = new BufferedInputStream(fileInputStream);
             ObjectInputStream objectInputStream  = new ObjectInputStream(bufferedInputStream)){

            Map<TopicPartition, OffsetAndMetadata> offsetsMapFromPath = (Map<TopicPartition, OffsetAndMetadata>) objectInputStream.readObject();
            logger.info("Offset Map read from the path is : {} ", offsetsMapFromPath);
            return offsetsMapFromPath;

        } catch (IOException | ClassNotFoundException e) {
            logger.error("Exception Occurred while reading the file {}", e.getMessage());
            return new HashMap<>();
        }
    }
}
