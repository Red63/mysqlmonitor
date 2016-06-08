package com.retail.datahub.kafka.sdk.producer;

import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;


public class KafkaProducerListener implements DisposableBean {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerListener.class);

    private Properties properties;
    private KafkaProducer producer;

    public KafkaProducerListener() {
        super();
    }

    public KafkaProducerListener(Properties properties) {
        super();
        this.properties = properties;
        init();
    }

    public void init() {
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new KafkaProducer(config);
        LOG.info("init KafkaProducerListener end...");
    }

    public void send(String topic, String msg) {
        producer.send(topic, msg);
    }

    public void send(String topic, String msgId, String partitionKey, String msg) {
        producer.send(topic, msgId, partitionKey, msg);
    }

    public void send(String topic, String partitionKey, String msg) {
        producer.send(topic, partitionKey, msg);
    }

    public void send(String topic, byte[] msg) {
        producer.send(topic, msg);
    }

    public void send(String topic, String msgId, String partitionKey, byte[] msg) {
        producer.send(topic, msgId, partitionKey, msg);
    }

    public void send(String topic, String partitionKey, byte[] msg) {
        producer.send(topic, partitionKey, msg);
    }

    public void send(List<KeyedMessage<byte[], byte[]>> messages) {
        producer.send(messages);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public void destroy() throws Exception{
        if (producer != null) {
            producer.close();
            LOG.info("kafka producer closed...");
        }
    }
}