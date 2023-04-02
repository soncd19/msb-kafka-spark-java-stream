package com.msb.stream.redis.serializer;

import com.msb.stream.redis.api.Deserializer;
import com.msb.stream.redis.exeception.DeserializationException;

import java.io.IOException;

public class StringValueDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] input) throws DeserializationException, IOException {
        if (input == null || input.length == 0) {
            return null;
        }
        return new String(input);
    }
}
