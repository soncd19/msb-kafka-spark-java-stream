package com.msb.stream.application;

import com.msb.stream.base.BaseApplication;
import com.msb.stream.connector.KafkaStreamConnector;
import com.msb.stream.connector.RedisConnector;
import com.msb.stream.utils.BroadcastTag;
import com.msb.stream.utils.ConfigMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkStreamJsonIn extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamJsonIn.class);

    public SparkStreamJsonIn(String configPath, String redisConfigPath, String jobName) {
        super(configPath, redisConfigPath, jobName);
    }

    @Override
    public void run() {

        try (SparkSession sparkSession = getSparkSession()) {
            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.setLogLevel("INFO");
            StreamingContext streamingContext = new StreamingContext(sparkContext,
                    Milliseconds.apply(100));
            JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
            JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaStreamConnector.createStream(javaStreamingContext);

            Broadcast<RedisConnector> broadcastRedisConnector = sparkContext.broadcast(
                    new RedisConnector(ConfigMap.config.get(ConfigMap.redisTopicIn), redisConfigPath),
                    BroadcastTag.classTag(RedisConnector.class));

            stream.foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    while (partitionOfRecords.hasNext()) {
                        ConsumerRecord<String, String> record = partitionOfRecords.next();
                        //todo: call redis check permission
                        broadcastRedisConnector.getValue().xAdd(record);
                    }
                });

                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            });


            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();

        } catch (Exception e) {
            logger.error("running streaming json in error {}", e.getMessage());
        } finally {
            close();
        }
    }
}
