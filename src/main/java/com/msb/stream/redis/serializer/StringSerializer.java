package com.msb.stream.redis.serializer;



import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.exeception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public void serialize(String value, OutputStream output) throws SerializationException, IOException {
        output.write(value.getBytes(StandardCharsets.UTF_8));
    }
}
