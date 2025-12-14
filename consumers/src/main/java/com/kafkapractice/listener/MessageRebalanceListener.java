package com.kafkapractice.listener;

import com.kafkapractice.consumer.MessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MessageRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageRebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        logger.info("onPartitionsRevoked: {}", collection);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        logger.info("onPartitionsAssigned: {}", collection);
    }
}
