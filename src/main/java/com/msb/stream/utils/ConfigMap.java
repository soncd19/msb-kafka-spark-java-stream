package com.msb.stream.utils;

import java.util.HashMap;
import java.util.Map;

public class ConfigMap {

    public static final String jobName = "jobName";
    public static final String kafkaServer = "kafkaServer";
    public static final String kafkaGroupId = "kafkaGroupId";
    public static final String autoOffsetReset = "autoOffsetReset";
    public static final String kafkaTopicsIn = "kafkaTopicsIn";
    public static final String kafkaTopicsOut = "kafkaTopicsOut";
    public static final String redisTopicIn = "redisTopicIn";
    public static final String redisTopicOut = "redisTopicOut";
    public static final String redisConfigPath = "redisConfigPath";
    public static final String redisConsumerGroup = "redisConsumerGroup";
    public static final String dataSourceUri = "dataSourceUri";
    public static final String dataSourceUser = "dataSourceUser";
    public static final String dataSourcePassword = "dataSourcePassword";
    public static final String dataSourceDriver = "dataSourceDriver";
    public static Map<String, String> config = new HashMap<>();

}
