package com.msb.stream.connector;


import com.msb.stream.redis.api.Deserializer;
import com.msb.stream.redis.api.DistributedMapCacheClient;
import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.serializer.CacheValueSerializer;
import com.msb.stream.redis.serializer.StringSerializer;
import com.msb.stream.redis.service.RedisDistributedMapCacheClientService;
import com.msb.stream.utils.StreamingUtils;
import com.msb.stream.utils.StringConstant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisConnector implements Serializable {
    private DistributedMapCacheClient distributedMapCacheClient = null;

    private final Document document;

    public RedisConnector(String redisPath) throws ParserConfigurationException,
            IOException, SAXException {
        this.document = StreamingUtils.parseDocument(redisPath);
    }

    private DistributedMapCacheClient init() throws Exception {
        return new RedisDistributedMapCacheClientService(document);
    }

    public void close() {
        distributedMapCacheClient.disable();
    }

    public void xAdd(String key, ConsumerRecord<String, String> record) throws Exception {
        Map<byte[], byte[]> messageBody = new HashMap<>();
        String value = record.value();
        messageBody.put(StringConstant.JSON_IN.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstant.VALUE_TS.getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.xAdd(key.getBytes(StandardCharsets.UTF_8), messageBody);
    }

    public void xAdd(String key, String message, String kafkaTopicOut) throws Exception {
        Map<byte[], byte[]> messageBody = new HashMap<>();

        messageBody.put(StringConstant.DATA.getBytes(StandardCharsets.UTF_8),
                message.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstant.KAFKA_TOPIC_OUT.getBytes(StandardCharsets.UTF_8),
                kafkaTopicOut.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstant.VALUE_TS.getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.xAdd(key.getBytes(StandardCharsets.UTF_8), messageBody);
    }

    public void put(String key, String value, Serializer<String> keySerializer,
                    Serializer<String> valueSerializer) throws IOException {
        distributedMapCacheClient.put(key, value, keySerializer, valueSerializer);
    }

    public List<String> getList(String key, Serializer<String> keySerializer,
                                Deserializer<String> valueStringDeserializer) throws IOException {
        return distributedMapCacheClient.getList(key, keySerializer, valueStringDeserializer);
    }
}
