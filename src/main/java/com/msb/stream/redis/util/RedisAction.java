package com.msb.stream.redis.util;

import org.springframework.data.redis.connection.RedisConnection;

import java.io.IOException;
import java.io.Serializable;

public interface RedisAction<T> extends Serializable {
    T execute(RedisConnection redisConnection) throws IOException;
}
