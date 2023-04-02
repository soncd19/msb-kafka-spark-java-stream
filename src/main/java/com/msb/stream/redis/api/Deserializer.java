package com.msb.stream.redis.api;



import com.msb.stream.redis.exeception.DeserializationException;

import java.io.IOException;


public interface Deserializer<T> {
    T deserialize(byte[] input) throws DeserializationException, IOException;
}
