package com.msb.stream;

import com.msb.redis.provider.redis.RedisConfig;
import com.msb.redis.provider.redis.streaming.ConsumerConfig;
import com.msb.redis.provider.redis.streaming.Latest$;
import com.msb.redis.provider.redis.streaming.RedisStreamingContext;
import com.msb.redis.provider.redis.streaming.StreamItem;
import com.msb.stream.application.SparkSubRedisSendKafka;
import com.msb.stream.pub.SparkKafkaProducer;
import com.msb.stream.utils.BroadcastTag;
import com.msb.stream.utils.ConfigMap;
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

public class SparkSubKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(SparkSubKafkaTest.class);
    public static void main(String[] args) {
        try  {

            SparkSession sparkSession = SparkSession.builder().appName("Redis11 Stream Example")
                    .master("local[*]")
                    .config("spark.redis.mode", "sentinel")
                    .config("spark.redis.sentinel.master", "transfer247")
                    .config("spark.redis.auth", "msb@2022")
                    .config("spark.redis.host", "10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379")
                    .config("spark.redis.port", "6379")
                    .getOrCreate();
            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.setLogLevel("INFO");

            Broadcast<SparkKafkaProducer> broadcastSparkKafkaProducer = sparkContext.broadcast(
                    new SparkKafkaProducer("10.1.66.33:9092"),
                    BroadcastTag.classTag(SparkKafkaProducer.class));

            StreamingContext streamingContext = new StreamingContext(sparkContext,
                    Milliseconds.apply(100));

            RedisStreamingContext redisStreamingContext = new RedisStreamingContext(streamingContext);

            ConsumerConfig consumerConfig = new ConsumerConfig("redis-out",
                    "my-group-spark",
                    "my-consumer-spark",
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
                        String data = item.fields().get("data").get();
                        String kafkaTopicOut = item.fields().get("kafka_topic_out").get();
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

        }
    }
}
