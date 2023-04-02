package com.msb.stream.redis.api;

import com.msb.stream.redis.api.RedisType;
import org.springframework.data.redis.connection.RedisConnection;

import java.io.Serializable;

public interface RedisConnectionPool extends Serializable {

    void onEnable();

    void onDisable();

    RedisConnection getConnection();

    RedisType getRedisType();
}
