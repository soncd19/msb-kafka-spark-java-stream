package com.msb.stream.redis.service;

import com.msb.stream.redis.util.RedisUtils;
import com.msb.stream.redis.api.PropertyContext;
import com.msb.stream.redis.api.RedisConnectionPool;
import com.msb.stream.redis.api.RedisType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;


public class RedisConnectionPoolService implements RedisConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisConnectionPoolService.class);
    private PropertyContext propertyContext;
    private RedisType redisType;
    private JedisConnectionFactory connectionFactory;

    public RedisConnectionPoolService(PropertyContext propertyContext) {
        this.propertyContext = propertyContext;
    }

    @Override
    public void onEnable() {
        String redisMode = propertyContext.getProperty(RedisUtils.REDIS_MODE).getValue();
        this.redisType = RedisType.fromDisplayName(redisMode);
    }

    @Override
    public void onDisable() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
            redisType = null;
            propertyContext = null;
        }
    }

    @Override
    public RedisConnection getConnection() {
        if (connectionFactory == null) {
            synchronized (this) {
                if (connectionFactory == null) {
                    connectionFactory = RedisUtils.createConnectionFactory(propertyContext, logger);
                }
            }
        }
        return connectionFactory.getConnection();
    }

    @Override
    public RedisType getRedisType() {
        return redisType;
    }
}
