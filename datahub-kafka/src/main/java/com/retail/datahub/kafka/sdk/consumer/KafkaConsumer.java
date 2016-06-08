package com.retail.datahub.kafka.sdk.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;
import com.retail.datahub.kafka.sdk.api.Preconditions;
import kafka.common.KafkaException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class KafkaConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

	private ConsumerConnector connector;
	private ConsumerIterator<byte[], byte[]> iterator;

	public KafkaConsumer(String topic, ConsumerConfig config) {
		try {
			this.connector = Consumer.createJavaConsumerConnector(config);
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			map.put(topic, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(map);

			// 在线程中消费指定流中的消息
			KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
			this.iterator = stream.iterator();
		} catch (Exception e){
			LOG.error(e.getMessage(), e);
			if (connector!=null) {
				connector.shutdown();
			}
			throw e;
		}
	}

	public void commitOffsets() {
		connector.commitOffsets();
	}

	protected void commitOffsets(boolean retryOnFailure) {
		connector.commitOffsets(retryOnFailure);
	}

	protected Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicMap) {
		Preconditions.checkNotNull(topicMap, "topicMap");
		return connector.createMessageStreams(topicMap);
	}

	protected <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicMap,
			Decoder<K> decoderK, Decoder<V> decoderV) {
		Preconditions.checkNotNull(topicMap, "topicMap");
		return connector.createMessageStreams(topicMap, decoderK, decoderV);
	}

	protected List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter filter) {
		throw new KafkaException("Not supported until now.");
		// return connector.createMessageStreamsByFilter(filter);
	}

	protected List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter filter, int numStreams) {
		throw new KafkaException("Not supported until now.");
		// return connector.createMessageStreamsByFilter(filter, numStreams);
	}

	protected <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter filter, int numStreams,
			Decoder<K> decoderK, Decoder<V> decoderV) {
		throw new KafkaException("Not supported until now.");
		// return connector.createMessageStreamsByFilter(filter, numStreams,
		// decoderK, decoderV);
	}

	public boolean hasNext() {
		boolean retVal = false;
		try {
			retVal = iterator.hasNext();
		} catch (Exception e) {
			connector.shutdown();
			LOG.error(e.getMessage(), e);
			throw e;
		}
		return retVal;
	}

	public MessageAndMetadata<byte[], byte[]> getData() {
		MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
		return messageAndMetadata;
	}


	public void shutdown() {
		if(connector!=null) {
			connector.shutdown();
		}
	}

}
