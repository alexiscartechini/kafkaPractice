package com.kafkapractice.consumer;

import com.kafkapractice.listener.OffsetRebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kafkapractice.listener.OffsetRebalanceListener.FILE_PATH;

public class SeekOnAssignConsumer extends AbstractKafkaConsumer<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(SeekOnAssignConsumer.class);
    private static final String TEST_TOPIC = "test-topic";
    private final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    public SeekOnAssignConsumer(Map<String, Object> consumerProperties) {
        super(consumerProperties);
    }

    public static void main(String[] args) {
        SeekOnAssignConsumer messageConsumer = new SeekOnAssignConsumer(baseConsumerProperties());
        messageConsumer.pollKafka();
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(TEST_TOPIC), new OffsetRebalanceListener(kafkaConsumer));

        try {
            while (running) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach(consumerRecord -> {
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
                    offsetMap.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1, null));
                });
                if (consumerRecords.count() > 0) {
                    writeOffsetsMapToPath(offsetMap);
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

    private void writeOffsetsMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetsMap){
        try (FileOutputStream fout = new FileOutputStream(FILE_PATH);
             ObjectOutputStream oos = new ObjectOutputStream(fout)) {
            oos.writeObject(offsetsMap);
            logger.info("Offsets Written Successfully!");
        } catch (IOException ex) {
            logger.error("Exception Occurred while writing the file {}", ex.getMessage());
        }
    }
}