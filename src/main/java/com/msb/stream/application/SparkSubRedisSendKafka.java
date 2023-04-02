package com.msb.stream.application;

import com.google.gson.JsonObject;
import com.msb.redis.provider.redis.RedisConfig;
import com.msb.redis.provider.redis.streaming.ConsumerConfig;
import com.msb.redis.provider.redis.streaming.Latest$;
import com.msb.redis.provider.redis.streaming.RedisStreamingContext;
import com.msb.redis.provider.redis.streaming.StreamItem;
import com.msb.stream.base.BaseApplication;
import com.msb.stream.call.CallProduceV2;
import com.msb.stream.connector.DatabaseConnector;
import com.msb.stream.connector.RedisConnector;
import com.msb.stream.pub.SparkKafkaProducer;
import com.msb.stream.utils.BroadcastTag;
import com.msb.stream.utils.ConfigMap;
import com.msb.stream.utils.StreamingUtils;
import com.msb.stream.utils.StringConstant;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.mutable.ArrayBuffer;

import java.util.UUID;

public class SparkSubRedisSendKafka extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(SparkSubRedisSendKafka.class);
    public SparkSubRedisSendKafka(String configPath, String redisConfigPath, String jobName) {
        super(configPath, redisConfigPath, jobName);
    }

    @Override
    public void run() {
        try (SparkSession sparkSession = getSparkSession()) {

//            SparkSession sparkSession = SparkSession.builder().appName("Redis Stream Example")
//                    .master("local[*]")
//                    .config("spark.redis.mode", "sentinel")
//                    .config("spark.redis.sentinel.master", "transfer247")
//                    .config("spark.redis.auth", "msb@2022")
//                    .config("spark.redis.host", "10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379")
//                    .config("spark.redis.port", "6379")
//                    .getOrCreate();
            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.setLogLevel("INFO");

            Broadcast<SparkKafkaProducer> broadcastSparkKafkaProducer = sparkContext.broadcast(
                    new SparkKafkaProducer(ConfigMap.config.get(ConfigMap.kafkaServer)),
                    BroadcastTag.classTag(SparkKafkaProducer.class));

            StreamingContext streamingContext = new StreamingContext(sparkContext,
                    Milliseconds.apply(100));

            RedisStreamingContext redisStreamingContext = new RedisStreamingContext(streamingContext);

            ConsumerConfig consumerConfig = new ConsumerConfig(ConfigMap.config.get(ConfigMap.redisTopicOut),
                    ConfigMap.config.get(ConfigMap.redisConsumerGroup),
                    UUID.randomUUID().toString(),
                    Latest$.MODULE$,
                    Option.apply(100),
                    100,
                    500L);

            ArrayBuffer<ConsumerConfig> seqList = new ArrayBuffer<>();
            seqList.$plus$eq(consumerConfig);

            InputDStream<StreamItem> streams = redisStreamingContext.createRedisXStream(
                    seqList, StorageLevel.MEMORY_AND_DISK_2(), RedisConfig.fromSparkConf(sparkContext.getConf()));

            streams.repartition(3).foreachRDD(rdd -> {
                JavaRDD<StreamItem> javaRDD = rdd.toJavaRDD();
                javaRDD.foreachPartition(partitionOfRecords -> {
                    while (partitionOfRecords.hasNext()) {
                        StreamItem item = partitionOfRecords.next();
                        String data = item.fields().get(StringConstant.DATA).get();
                        String kafkaTopicOut = item.fields().get(StringConstant.KAFKA_TOPIC_OUT).get();
                        broadcastSparkKafkaProducer.getValue().send(data, kafkaTopicOut);
                    }
                });

                return null;
            });

            streamingContext.start();
            streamingContext.awaitTermination();

        } catch (Exception ex) {
            logger.error("spark subcribe to redis error: {}", ex.getMessage());
        } finally {
            close();
        }
    }
}
