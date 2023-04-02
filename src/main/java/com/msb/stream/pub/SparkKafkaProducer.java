package com.msb.stream.pub;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Future;

public class SparkKafkaProducer implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkKafkaProducer.class);
    private String outTopic;
    private String kafkaBroker;
    private KafkaProducer<byte[], byte[]> producer;


    public SparkKafkaProducer(String kafkaBroker) {
        this(kafkaBroker, "default");
    }

    public SparkKafkaProducer(String kafkaBroker, String outTopic) {
        this.outTopic = outTopic;
        this.kafkaBroker = kafkaBroker;
    }

    public KafkaProducer<byte[], byte[]> init(String kafkaBroker) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "20971520");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        return new KafkaProducer<>(properties);
    }

    public void send(String message) {
        try {
            if (producer == null) {
                producer = init(kafkaBroker);
            }
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(outTopic, getKey(System.currentTimeMillis()),
                    getMessage(message));
            Future<RecordMetadata> future = producer.send(record, new DummyCallback());
        } catch (Exception ex) {
            logger.error("Sending message to kafka error: {}", ex.getMessage());
        }
    }

    public void send(String message, String topic) {
        try {
            if (producer == null) {
                producer = init(kafkaBroker);
            }
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, getKey(System.currentTimeMillis()),
                    getMessage(message));
            Future<RecordMetadata> future = producer.send(record, new DummyCallback());
        } catch (Exception ex) {
            logger.error("Sending message to kafka error: {}", ex.getMessage());
        }
    }

    public void close() {
        producer.close();
    }

    private static byte[] getMessage(String message) {
        return serialize(message);
    }

    private static byte[] getKey(long i) {
        return serialize(String.valueOf(i));
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
    }


    private static class DummyCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                logger.error("Error while producing message to topic : {}", recordMetadata.topic());
            } else {
                logger.info("sent message to topic: {}, partition: {}, offset: {} ", recordMetadata.topic(), +recordMetadata.partition(), recordMetadata.offset());
            }
        }
    }
}
