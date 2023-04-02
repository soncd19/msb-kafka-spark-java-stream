package com.msb.stream.redis.util;



import com.msb.stream.redis.api.Deserializer;
import com.msb.stream.redis.api.DistributedMapCacheClient;
import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.serializer.CacheValueDeserializer;
import com.msb.stream.redis.serializer.CacheValueSerializer;
import com.msb.stream.redis.serializer.StringSerializer;
import com.msb.stream.redis.serializer.StringValueDeserializer;
import com.msb.stream.redis.service.RedisDistributedMapCacheClientService;
import com.msb.stream.utils.StreamingUtils;
import org.w3c.dom.Document;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String redisPath = System.getProperty("user.dir") + "/config/redis-config.xml";

        Document document = StreamingUtils.parseDocument(redisPath);
        Serializer<String> keySerializer = new StringSerializer();
        Serializer<byte[]> valueSerializer = new CacheValueSerializer();
        Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();
        Deserializer<String> valueStringDeserializer = new StringValueDeserializer();
        DistributedMapCacheClient distributedMapCacheClient = new RedisDistributedMapCacheClientService(document);
        String key = "soncdtestnewpool";
        String value1 = "soncd1";
        String value2 = "soncd2";
        distributedMapCacheClient.put(key, value1.getBytes(StandardCharsets.UTF_8), keySerializer, valueSerializer);
        distributedMapCacheClient.put(key, value2.getBytes(StandardCharsets.UTF_8), keySerializer, valueSerializer);
        //distributedMapCacheClient.set(key, value.getBytes(StandardCharsets.UTF_8), 60L, keySerializer, valueSerializer);
        //byte[] values = distributedMapCacheClient.get(key, keySerializer, valueDeserializer);


        List<String> valuesList = distributedMapCacheClient.getList(key, keySerializer, valueStringDeserializer);

        valuesList.forEach(System.out::println);

    }
}
