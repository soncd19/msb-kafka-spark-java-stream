package com.msb.stream.redis.standard;

import com.msb.stream.redis.util.PropertyDescriptor;
import com.msb.stream.redis.util.RedisUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StandardParserConfig implements Serializable {

    public static Map<PropertyDescriptor, String> parser(Document document) throws Exception {

        Map<PropertyDescriptor, String> properties = new HashMap<>();
        document.getDocumentElement().normalize();
        Element element = document.getDocumentElement();
        String redisMode = element.getElementsByTagName("redisMode").item(0).getTextContent();
        String connectionString = element.getElementsByTagName("connectionString").item(0).getTextContent();
        String communicationTimeOut = element.getElementsByTagName("communicationTimeOut").item(0).getTextContent();
        String clusterMaxRedirects = element.getElementsByTagName("clusterMaxRedirects").item(0).getTextContent();
        String sentinelMaster = element.getElementsByTagName("sentinelMaster").item(0).getTextContent();
        String password = element.getElementsByTagName("password").item(0).getTextContent();
        String poolMaxTotal = element.getElementsByTagName("poolMaxTotal").item(0).getTextContent();
        String pollMaxIdle = element.getElementsByTagName("pollMaxIdle").item(0).getTextContent();
        String pollMinIdle = element.getElementsByTagName("pollMinIdle").item(0).getTextContent();
        String poolBlockWhenExhausted = element.getElementsByTagName("poolBlockWhenExhausted").item(0).getTextContent();
        String poolMaxWaitTime = element.getElementsByTagName("poolMaxWaitTime").item(0).getTextContent();
        String poolMinEvictableIdleTime = element.getElementsByTagName("poolMinEvictableIdleTime").item(0).getTextContent();
        String poolTimeBetweenEvictionRuns = element.getElementsByTagName("poolTimeBetweenEvictionRuns").item(0).getTextContent();
        String poolNumTestsPerEvictionRun = element.getElementsByTagName("poolNumTestsPerEvictionRun").item(0).getTextContent();
        String poolTestOnCreate = element.getElementsByTagName("poolTestOnCreate").item(0).getTextContent();
        String poolTestOnBorrow = element.getElementsByTagName("poolTestOnBorrow").item(0).getTextContent();
        String poolTestOnReturn = element.getElementsByTagName("poolTestOnReturn").item(0).getTextContent();
        String poolTestWhileIdle = element.getElementsByTagName("poolTestWhileIdle").item(0).getTextContent();

        properties.put(RedisUtils.REDIS_MODE, redisMode);
        properties.put(RedisUtils.CONNECTION_STRING, connectionString);
        properties.put(RedisUtils.COMMUNICATION_TIMEOUT, communicationTimeOut);
        properties.put(RedisUtils.CLUSTER_MAX_REDIRECTS, clusterMaxRedirects);
        properties.put(RedisUtils.SENTINEL_MASTER, sentinelMaster);
        properties.put(RedisUtils.PASSWORD, password);
        properties.put(RedisUtils.POOL_MAX_TOTAL, poolMaxTotal);
        properties.put(RedisUtils.POOL_MAX_IDLE, pollMaxIdle);
        properties.put(RedisUtils.POOL_MIN_IDLE, pollMinIdle);
        properties.put(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED, poolBlockWhenExhausted);
        properties.put(RedisUtils.POOL_MAX_WAIT_TIME, poolMaxWaitTime);
        properties.put(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME, poolMinEvictableIdleTime);
        properties.put(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS, poolTimeBetweenEvictionRuns);
        properties.put(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN, poolNumTestsPerEvictionRun);
        properties.put(RedisUtils.POOL_TEST_ON_CREATE, poolTestOnCreate);
        properties.put(RedisUtils.POOL_TEST_ON_BORROW, poolTestOnBorrow);
        properties.put(RedisUtils.POOL_TEST_ON_RETURN, poolTestOnReturn);
        properties.put(RedisUtils.POOL_TEST_WHILE_IDLE, poolTestWhileIdle);

        return properties;
    }

}
