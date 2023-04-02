package com.msb.stream.application;

import com.google.gson.JsonObject;
import com.msb.stream.base.BaseApplication;
import com.msb.stream.connector.RedisConnector;
import com.msb.stream.redis.api.Serializer;
import com.msb.stream.redis.serializer.StringSerializer;
import com.msb.stream.utils.BroadcastTag;
import com.msb.stream.utils.StreamingUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkLoadPermission extends BaseApplication {
    private static final Logger logger = LoggerFactory.getLogger(SparkLoadPermission.class);

    public SparkLoadPermission(String configPath, String redisConfigPath, String jobName) {
        super(configPath, redisConfigPath, jobName);
    }

    @Override
    public void run() {
        try (SparkSession sparkSession = getSparkSession()) {

            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.setLogLevel("INFO");

            Serializer<String> keySerializer = new StringSerializer();
            Serializer<String> valueSerializer = new StringSerializer();
            Broadcast<RedisConnector> broadcastRedisConnector = sparkContext.broadcast(
                    new RedisConnector(redisConfigPath),
                    BroadcastTag.classTag(RedisConnector.class));
            Dataset<Row> tbl1 = readTable("abc");
            Dataset<Row> tbl2 = readTable("abc");
            Dataset<Row> join = tbl1.join(tbl1, tbl1.col("1").equalTo(tbl2.col("2")), "inner");
            join.toJSON().foreachPartition(iterator -> {
                while (iterator.hasNext()) {
                    String value = iterator.next();
                    JsonObject json = StreamingUtils.toJson(value);
                    String userName = json.get("userName").getAsString();
                    String tblName = json.get("tableName").getAsString();
                    broadcastRedisConnector.getValue().put(userName, tblName, keySerializer, valueSerializer);
                }
            });
        } catch (Exception e) {
            logger.error("running load permission user in error {}", e.getMessage());
        } finally {
            close();
        }
    }
}
