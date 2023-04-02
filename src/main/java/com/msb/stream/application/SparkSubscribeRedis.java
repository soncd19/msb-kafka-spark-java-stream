package com.msb.stream.application;

import com.google.gson.JsonObject;
import com.msb.redis.provider.redis.RedisConfig;
import com.msb.redis.provider.redis.streaming.*;
import com.msb.stream.call.CallProduceV2;
import com.msb.stream.base.BaseApplication;
import com.msb.stream.connector.RedisConnector;
import com.msb.stream.executor.ExportService;
import com.msb.stream.utils.*;
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

public class SparkSubscribeRedis extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(SparkSubscribeRedis.class);

    public SparkSubscribeRedis(String configPath, String redisConfigPath, String jobName) {
        super(configPath, redisConfigPath, jobName);
    }

    @Override
    public void run() {
        try /*(SparkSession sparkSession = getSparkSession())*/ {

            SparkSession sparkSession = SparkSession.builder().appName("Redis Stream Example")
                    .master("local[*]")
                    .config("spark.redis.mode", "sentinel")
                    .config("spark.redis.sentinel.master", "transfer247")
                    .config("spark.redis.auth", "msb@2022")
                    .config("spark.redis.host", "10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379")
                    .config("spark.redis.port", "6379")
                    .getOrCreate();

            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.setLogLevel("INFO");

            Broadcast<CallProduceV2> broadcastCallProducerV2 = sparkContext.broadcast(
                    new CallProduceV2(configPath),
                    BroadcastTag.classTag(CallProduceV2.class));

            Broadcast<ExportService> exportService = sparkContext.broadcast(
                    new ExportService(),
                    BroadcastTag.classTag(ExportService.class));

            Broadcast<RedisConnector> broadcastRedisConnector = sparkContext.broadcast(
                    new RedisConnector(redisConfigPath),
                    BroadcastTag.classTag(RedisConnector.class));

            StreamingContext streamingContext = new StreamingContext(sparkContext,
                    Milliseconds.apply(100));

            RedisStreamingContext redisStreamingContext = new RedisStreamingContext(streamingContext);

            ConsumerConfig consumerConfig = new ConsumerConfig(ConfigMap.config.get(ConfigMap.redisTopicIn),
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
                        JsonObject json = StreamingUtils.toJson(item.fields().get(StringConstant.JSON_IN).get());
                        String kafkaTopicOut = json.getAsJsonObject(StringConstant.USER_HEADER)
                                .get(StringConstant.KAFKA_TOPIC).getAsString();

                        String isExport = json.getAsJsonObject(StringConstant.USER_HEADER).get(StringConstant.SPECIAL_MISSION).getAsString();

                        JsonObject fileArray = json.getAsJsonObject(StringConstant.USER_HEADER)
                                .getAsJsonObject(StringConstant.SPECIAL_MESSAGE).getAsJsonArray(StringConstant.FILE_ARRAY)
                                .get(0).getAsJsonObject();

                        String pathName = fileArray.get(StringConstant.PATH_NAME).getAsString();
                        String fileName = fileArray.get(StringConstant.FILE_NAME).getAsString();

                        switch (isExport) {
                            case StringConstant.EXPORT_EXCEL:
                                /*export excel*/
                                String responseExportData = broadcastCallProducerV2.getValue()
                                        .process(StringConstant.DATAMAN_PR_CRUD_HANDLER001, json);
                                exportService.getValue().process(pathName, fileName, responseExportData);
                                break;
                            case StringConstant.IMPORT_EXCEL:
                                //todo import excel DATAMAN.PR_CRUD_HANDLER002
                                // String response = broadcastCallProducerV2.getValue().process("DATAMAN.PR_CRUD_HANDLER002",json);
                                break;
                            case StringConstant.NO:
                                String responseGetData = broadcastCallProducerV2
                                        .getValue()
                                        .process(StringConstant.PR_OUPUT_SERVICES, json);

                                broadcastRedisConnector
                                        .getValue()
                                        .xAdd(ConfigMap.config.get(ConfigMap.redisTopicOut), responseGetData, kafkaTopicOut);
                                break;
                        }

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
