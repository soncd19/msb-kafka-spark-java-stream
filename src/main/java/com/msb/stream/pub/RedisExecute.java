package com.msb.stream.pub;

import com.msb.stream.connector.RedisConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class RedisExecute implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RedisExecute.class);
    private RedisConnector redisConnector;
    private final String redisTopic;

    public RedisExecute(String redisTopic, String redisConfigPath) throws Exception {
//        init(redisConfigPath);
        this.redisTopic = redisTopic;
    }

//    private void init(String redisConfigPath) throws Exception {
//        this.redisConnector = new RedisConnector(redisConfigPath);
//    }
//
//    public void close() {
//        redisConnector.close();
//    }
//
//    public void process(ConsumerRecord<String, String> record) throws IOException {
//        logger.info("record.value(): {}", record.value());
//        redisConnector.xAdd(redisTopic, record);
//    }
}
