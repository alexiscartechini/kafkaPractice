package com.kafkapractice.deserializer;

import com.kafkapractice.domain.Item;
import org.apache.kafka.common.serialization.Deserializer;

public class ItemDeserializer implements Deserializer<Item> {

    @Override
    public Item deserialize(String s, byte[] bytes) {
        return null;
    }
}
