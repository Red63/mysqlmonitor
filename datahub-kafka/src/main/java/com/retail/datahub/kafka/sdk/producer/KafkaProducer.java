package com.retail.datahub.kafka.sdk.producer;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

import com.retail.datahub.kafka.sdk.api.Preconditions;
import com.retail.datahub.kafka.sdk.common.KafkaMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message producer
 *
 */
public final class KafkaProducer {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
	private long autoIncrementKey;
	private Producer<byte[], byte[]> producer;

	public KafkaProducer(ProducerConfig config) {
		try {
			this.producer = new Producer<byte[], byte[]>(config);
			this.autoIncrementKey = 1;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			if(producer!=null) {
				producer.close();
			}
			throw e;
		}
	}


	/**
	 * send batch message, List<>
	 * @param messages
	 */
	public void send(List<KeyedMessage<byte[], byte[]>> messages) {
		Preconditions.checkNotNull(messages, "messages");
		producer.send(messages);
	}

	/**
	 * send single message, String
	 * @param message
	 */
	public void send(String topic, String message) {
		Preconditions.checkNotNull(message, "message");
		try {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, UUID.randomUUID().toString().getBytes("UTF-8"), String.valueOf(this.autoIncrementKey), message.getBytes("UTF-8")));
			this.autoIncrementKey++;
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}

	}

	public void send(String topic, String partitionKey, String message) {
		this.send(topic, UUID.randomUUID().toString(), partitionKey, message);
	}

	public void send(String topic, String msgId, String partitionKey, String message) {
		Preconditions.checkNotNull(message, "message");
		try {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, msgId.getBytes("UTF-8"), partitionKey, message.getBytes("UTF-8")));
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}

	}

	public void send(String topic, KafkaMessage message) {
		Preconditions.checkNotNull(message, "message");
		try {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, message.getKey().getBytes("UTF-8"), message.getMsg()));
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	/**
	 * send single message, byte[]
	 * @param message
	 */
	public void send(String topic, byte[] message) {
		Preconditions.checkNotNull(message, "message");
		try {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, UUID.randomUUID().toString().getBytes("UTF-8"), String.valueOf(this.autoIncrementKey), message));
			this.autoIncrementKey++;
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void send(String topic, String partitionKey, byte[] message) {
		this.send(topic, UUID.randomUUID().toString(), partitionKey, message);
	}

	public void send(String topic, String msgId, String partitionKey, byte[] message) {
		Preconditions.checkNotNull(message, "message");
		try {
			producer.send(new KeyedMessage<byte[], byte[]>(topic, msgId.getBytes("UTF-8"), partitionKey, message));
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void close() throws Exception{
		if (producer!=null) {
			producer.close();
		}
	}

}
