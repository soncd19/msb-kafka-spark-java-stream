package com.msb.stream.base;

import com.msb.stream.utils.ConfigMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public abstract class SparkSessionWrapper implements Serializable {

    private SparkSession sparkSession;

    public void initSparkSession() {
        SparkConf sparkConf = new SparkConf().setAppName(ConfigMap.config.get(ConfigMap.jobName));
        this.sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }

    public int repartitionNumber() {
        int coresMax = Integer.parseInt(sparkSession.sparkContext().conf()
                .get("spark.cores.max"));
        return coresMax * 5;
    }
}
