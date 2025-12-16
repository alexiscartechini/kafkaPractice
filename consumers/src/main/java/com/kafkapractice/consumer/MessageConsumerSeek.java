package com.kafkapractice.consumer;

import com.kafkapractice.listener.MessageRebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import static com.kafkapractice.listener.MessageRebalanceListener.serialiaziedFilePath;

public class MessageConsumerSeek {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerSeek.class);
    private KafkaConsumer<String, String> kafkaConsumer;
    private String topicName = "test-topic";
    private Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    public static void main(String[] args) {
        MessageConsumerSeek messageConsumer = new MessageConsumerSeek(buildConsumerProperties());
        messageConsumer.pollKafka();
    }

    public MessageConsumerSeek(Map<String, Object> consumerProperties){
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    public static  Map<String, Object> buildConsumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:29092,localhost:29093,localhost:29094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup2");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public void pollKafka(){
        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer));

        try {
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((record) -> {
                    logger.info("Consumer Record Key is {} and message is \"{}\" from partition {}",
                            record.key(), record.value(), record.partition());
                    offsetMap.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()+1, null));
                });
                if (consumerRecords.count()>0){
                    writeOffsetsMapToPath(offsetMap);
//                    kafkaConsumer.commitSync(offsetMap);
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

    private void writeOffsetsMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetsMap) throws IOException {

        FileOutputStream fout = null;
        ObjectOutputStream oos = null;
        try {
            fout = new FileOutputStream(serialiaziedFilePath);
            oos = new ObjectOutputStream(fout);
            oos.writeObject(offsetsMap);
            logger.info("Offsets Written Successfully!");
        } catch (Exception ex) {
            logger.error("Exception Occurred while writing the file : " + ex);
        } finally {
            if(fout!=null)
                fout.close();
            if(oos!=null)
                oos.close();
        }
    }
}
