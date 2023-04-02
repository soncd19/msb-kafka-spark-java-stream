package com.msb.stream.redis.serializer;


import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.exeception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;

public class CacheValueSerializer implements Serializer<byte[]> {
    @Override
    public void serialize(byte[] value, OutputStream output) throws SerializationException, IOException {
        output.write(value);
    }
}
