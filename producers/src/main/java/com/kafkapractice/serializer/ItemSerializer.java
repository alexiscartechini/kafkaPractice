package com.kafkapractice.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkapractice.domain.Item;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemSerializer implements Serializer<Item> {

    private static final Logger logger = LoggerFactory.getLogger(ItemSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Item item) {
        try {
            logger.info("Inside serialization logic");
            return objectMapper.writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            logger.error("Something happened while serializing: {}, error message is: {}", item, e.getMessage());
            return new byte[0];
        }
    }
}