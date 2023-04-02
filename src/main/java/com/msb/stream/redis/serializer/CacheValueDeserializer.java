package com.msb.stream.redis.serializer;


import com.msb.stream.redis.api.Deserializer;
import com.msb.stream.redis.exeception.DeserializationException;

import java.io.IOException;

public class CacheValueDeserializer implements Deserializer<byte[]> {
    @Override
    public byte[] deserialize(byte[] input) throws DeserializationException {
        if (input == null || input.length == 0) {
            return null;
        }
        return input;
    }
}
