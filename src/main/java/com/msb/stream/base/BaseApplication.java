package com.msb.stream.base;

import com.msb.stream.utils.ConfigMap;
import com.msb.stream.utils.PropertiesFileReader;

import java.util.Properties;

public abstract class BaseApplication extends SparkSessionWrapper {
    private final String jobName;
    protected final String configPath;
    protected final String redisConfigPath;

    public BaseApplication(String configPath, String redisConfigPath, String jobName) {
        this.jobName = jobName;
        this.configPath = configPath;
        this.redisConfigPath = redisConfigPath;
        loadConfig(configPath);
        initSparkSession();
    }

    private void loadConfig(String configPath) {
        Properties properties = PropertiesFileReader.readConfig(configPath);
        String kafkaServer = properties.getProperty("kafka.server");
        String kafkaTopicsIn = properties.getProperty("kafka.topics.in");
        String kafkaTopicsOut = properties.getProperty("kafka.topics.out");
        String redisTopicIn = properties.getProperty("redis.topic.in");
        String redisTopicOut = properties.getProperty("redis.topic.out");
        String kafkaGroupId = properties.getProperty("kafka.groupId");
        String autoOffsetReset = properties.getProperty("auto.offset.reset");
        String redisConfigPath = properties.getProperty("redis.config.path");
        String redisConsumerGroup = properties.getProperty("redis.consumer.group");
        String dataSourceUri = properties.getProperty("dataSource.uri");
        String dataSourceUser = properties.getProperty("dataSource.user");
        String dataSourcePassword = properties.getProperty("dataSource.password");
        String dataSourceDriver = properties.getProperty("dataSource.driver");
        ConfigMap.config.put(ConfigMap.jobName, jobName);
        ConfigMap.config.put(ConfigMap.kafkaServer, kafkaServer);
        ConfigMap.config.put(ConfigMap.kafkaTopicsIn, kafkaTopicsIn);
        ConfigMap.config.put(ConfigMap.kafkaTopicsOut, kafkaTopicsOut);
        ConfigMap.config.put(ConfigMap.redisTopicIn, redisTopicIn);
        ConfigMap.config.put(ConfigMap.redisTopicOut, redisTopicOut);
        ConfigMap.config.put(ConfigMap.kafkaGroupId, kafkaGroupId);
        ConfigMap.config.put(ConfigMap.autoOffsetReset, autoOffsetReset);
        ConfigMap.config.put(ConfigMap.redisConfigPath, redisConfigPath);
        ConfigMap.config.put(ConfigMap.redisConsumerGroup, redisConsumerGroup);
        ConfigMap.config.put(ConfigMap.dataSourceUri, dataSourceUri);
        ConfigMap.config.put(ConfigMap.dataSourceUser, dataSourceUser);
        ConfigMap.config.put(ConfigMap.dataSourcePassword, dataSourcePassword);
        ConfigMap.config.put(ConfigMap.dataSourceDriver, dataSourceDriver);
    }

    public abstract void run();

}
