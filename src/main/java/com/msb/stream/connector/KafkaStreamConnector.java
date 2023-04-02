package com.msb.stream.connector;

import com.msb.stream.utils.ConfigMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public interface KafkaStreamConnector {

    static JavaInputDStream<ConsumerRecord<String, String>> createStream(JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", ConfigMap.config.get(ConfigMap.kafkaServer));
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", ConfigMap.config.get(ConfigMap.kafkaGroupId));
        kafkaParams.put("auto.offset.reset", ConfigMap.config.get(ConfigMap.autoOffsetReset));
        kafkaParams.put("enable.auto.commit", true);
        Collection<String> topics = Collections.singletonList(ConfigMap.config.get(ConfigMap.kafkaTopicsIn));

        return KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

    }
}
