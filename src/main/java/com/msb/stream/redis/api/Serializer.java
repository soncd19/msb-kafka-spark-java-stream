package com.msb.stream.redis.api;



import com.msb.stream.redis.exeception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;

public interface Serializer<T> {
    void serialize(T value, OutputStream output) throws SerializationException, IOException;
}
