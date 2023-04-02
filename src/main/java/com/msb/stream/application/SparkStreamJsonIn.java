package com.msb.stream.application;

import com.google.gson.JsonObject;
import com.msb.stream.base.BaseApplication;
import com.msb.stream.connector.KafkaStreamConnector;
import com.msb.stream.connector.RedisConnector;
import com.msb.stream.redis.api.Deserializer;
import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.serializer.StringSerializer;
import com.msb.stream.redis.serializer.StringValueDeserializer;
import com.msb.stream.utils.BroadcastTag;
import com.msb.stream.utils.ConfigMap;
import com.msb.stream.utils.StreamingUtils;
import com.msb.stream.utils.StringConstant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

import java.util.List;


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
                    new RedisConnector(redisConfigPath),
                    BroadcastTag.classTag(RedisConnector.class));

            Serializer<String> keySerializer = new StringSerializer();
            Deserializer<String> valueStringDeserializer = new StringValueDeserializer();

            stream.foreachRDD(rdd -> {
                rdd.foreachPartition(partitionOfRecords -> {
                    while (partitionOfRecords.hasNext()) {
                        ConsumerRecord<String, String> record = partitionOfRecords.next();
                        JsonObject json = StreamingUtils.toJson(record.value());
                        String user = json.getAsJsonObject(StringConstant.USER_HEADER).get(StringConstant.USER).getAsString();
                        String tableName = json.getAsJsonObject(StringConstant.USER_HEADER).get(StringConstant.TABLE_NAME).getAsString();
                        List<String> roleTable = broadcastRedisConnector.getValue().getList(user, keySerializer, valueStringDeserializer);
                        if (roleTable.contains(tableName)) {
                            broadcastRedisConnector.getValue().xAdd(ConfigMap.config.get(ConfigMap.redisTopicIn), record);
                        } else {

                        }
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
