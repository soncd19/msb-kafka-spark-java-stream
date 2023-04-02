#!/bin/bash
user_dir="$(cd -P -- "$(dirname -- "$0")" && pwd -P)"
echo ${user_dir}

/data/spark-3.2.3-bin-hadoop3.2/bin/spark-submit \
 --name "Spark Sending Kafka" \
 --master spark://sparkmaster157:7077,sparkworker158:7077 \
 --conf "spark.redis.mode=sentinel" \
 --conf "spark.redis.sentinel.master=transfer247" \
 --conf "spark.redis.auth=msb@2022" \
 --conf "spark.redis.host=10.0.65.237:26379,10.0.65.238:26379,10.0.65.239:26379" \
 --conf "spark.redis.port=6379" \
 --conf "spark.driver.extraJavaOptions=-Dlogback.configurationFile=${user_dir}/config/logback.xml" \
  --deploy-mode client \
  --executor-memory 1G \
   --total-executor-cores 3 \
    --driver-memory 1G \
    --driver-class-path ${user_dir}/config/ojdbc8-18.15.0.0.jar \
      --class com.msb.stream.App \
       ${user_dir}/msb-kafka-spark-java-stream-1.0-jar-with-dependencies.jar "process_spark_sub_redis_send_kafka" \
       "${user_dir}/config/application.properties" \
       "${user_dir}/config/redis-config.xml"
