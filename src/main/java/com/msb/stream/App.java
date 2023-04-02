package com.msb.stream;

import com.msb.stream.application.SparkStreamJsonIn;
import com.msb.stream.application.SparkSubRedisSendKafka;
import com.msb.stream.application.SparkSubscribeRedis;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class App {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        showCommand();
        String command = args.length == 0 ? "process_spark_sub_redis" : args[0];
        String configPath = args.length == 0 ? System.getProperty("user.dir") + "/config/application.properties" : args[1];
        String redisConfigPath = args.length == 0 ? System.getProperty("user.dir") + "/config/redis-config.xml" : args[2];

        switch (command) {
            case "process_json_in":
                new SparkStreamJsonIn(configPath, redisConfigPath, "SparkStreamJsonIn").run();
                break;
            case "process_spark_sub_redis":
                new SparkSubscribeRedis(configPath, redisConfigPath, "SparkSubscribeRedis").run();
                break;
            case "process_spark_sub_redis_send_kafka":
                new SparkSubRedisSendKafka(configPath, redisConfigPath, "SparkSubRedisSendKafka").run();
                break;
        }

    }

    private static void showCommand() {
        String introduction = "WELCOME TO MSB KAFKA SERVICE SPARK STREAM\n"
                + "|- 1 argument: Spark job command\n"
                + "|- 2 argument: Application configuration path\n"
                + "|Available spark job commands:\n"
                + "|- process_json_in => SparkStreamJsonIn\n"
                + "| - process_spark_sub_redis => SparkSubscribeRedis\n";

        System.out.print(introduction);
    }
}
