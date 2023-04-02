package com.msb.stream.connector;


import com.msb.stream.redis.api.DistributedMapCacheClient;
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
import java.util.Map;

public class RedisConnector implements Serializable {
    private DistributedMapCacheClient distributedMapCacheClient = null;
    private final String topicOut;
    private final Document document;

    public RedisConnector(String topicOut, String redisPath) throws ParserConfigurationException,
            IOException, SAXException {
        this.topicOut = topicOut;
        this.document = StreamingUtils.parseDocument(redisPath);
    }

    private DistributedMapCacheClient init() throws Exception {
        return new RedisDistributedMapCacheClientService(document);
    }

    public void close() {
        distributedMapCacheClient.disable();
    }

    public void xAdd(ConsumerRecord<String, String> record) throws Exception {
        Map<byte[], byte[]> messageBody = new HashMap<>();
        String value = record.value();
        messageBody.put(StringConstant.JSON_IN.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        messageBody.put(StringConstant.VALUE_TS.getBytes(StandardCharsets.UTF_8),
                String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        if (distributedMapCacheClient == null) {
            distributedMapCacheClient = init();
        }
        distributedMapCacheClient.xAdd(topicOut.getBytes(StandardCharsets.UTF_8), messageBody);
    }

    public void xAdd(String message, String kafkaTopicOut) throws Exception {
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
        distributedMapCacheClient.xAdd(topicOut.getBytes(StandardCharsets.UTF_8), messageBody);
    }
}
