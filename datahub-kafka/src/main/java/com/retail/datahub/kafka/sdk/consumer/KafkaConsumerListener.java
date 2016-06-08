package com.retail.datahub.kafka.sdk.consumer;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Splitter;
import kafka.common.KafkaException;
import kafka.consumer.ConsumerConfig;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;

public class KafkaConsumerListener implements DisposableBean {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerListener.class);
    private static final int DEFAULT_MAX_RETRIES = 15;
    private static final int DEFAULT_MIN_CONSUMER_NUM = 1;
    private static final int DEFUALT_MAX_CONSUMSER_NUM = 20;

    private int[] RETRY_INTERVAL_ARR = new int[]{10, 30, 60, 120, 180,
            240, 300, 360, 420, 480,
            540, 600, 1200, 1800, 3600};  // unit: seconds

    private KafkaMessageListener messageHandler;
    private List<KafkaConsumer> consumers = new ArrayList<>();
    private Properties properties;

    private List<Properties> conflis;
    private String topics;

    private List<String> topicList;


    public KafkaConsumerListener() {
        super();
    }

    public KafkaConsumerListener(Properties properties) {
        super();
        this.properties = properties;
        init();
    }

    public KafkaConsumerListener(List<Properties> conflis) {
        super();
        this.conflis = conflis;
        init();
    }

	/*public void init() {
        final String topic = properties.getProperty("topic");
		final int consumerSize = parseConsumerSize(properties);
		ConsumerConfig config = new ConsumerConfig(properties);

		LOG.info("init KafkaConsumerListener starting...  [topic:" + topic + "]");
		for (int i = 0; i < consumerSize; i++) {
			final KafkaConsumer consumer = new KafkaConsumer(topic, config);
			final KafkaMessageListener msgHandler = (KafkaMessageListener) messageHandler.clone();
			final boolean isUniformRetry = msgHandler.isUseUniformRetry();
			final long uniformRetryMs = msgHandler.getUniformIntervalMs();

			if (msgHandler == null) {
				throw new KafkaException("Can not build messageHandler for the topic:"+topic);
			}
			msgHandler.setTopic(topic);
			Thread consumerThread = new Thread(new Runnable() {

				public void run() {
					MessageAndMetadata<byte[], byte[]> messageAndMetadata = null;
					try {
						while (consumer.hasNext() && !Thread.interrupted()) {
							messageAndMetadata = consumer.getData();

							try {
								if (msgHandler.useByteArray()) {
									// if not override onMsg(String msgId, byte[] msg), maybe a null invoke.
									msgHandler.onMsg(new String(messageAndMetadata.key(), "UTF-8"), messageAndMetadata.message());
								} else {
									msgHandler.onMsg(new String(messageAndMetadata.key(), "UTF-8"), new String(messageAndMetadata.message(), "UTF-8"));
								}

							} catch (Exception ee) {
								LOG.error(ee.getMessage(), ee);
								long retries = msgHandler.getErrorMaxRetries();
								// range from 1 ~ DEFAULT_MAX_RETRIES do as it, other retry DEFAULT_MAX_RETRIES times
								if (retries != -1) {
									if (retries <= 0 || retries > DEFAULT_MAX_RETRIES) {
										retries = DEFAULT_MAX_RETRIES;
									}

									for (int i = 0; i < retries; i++) {
										try {
											if(isUniformRetry) {
												LOG.info(String.format(Thread.currentThread().getName()+"->[USING RETRY] %s times, for consuming topic:%s, sleeping %s seconds", (i + 1), topic, (uniformRetryMs/1000)));
												Thread.sleep(uniformRetryMs);
											} else {
												LOG.info(String.format(Thread.currentThread().getName()+"->[USING RETRY] %s times, for consuming topic:%s, sleeping %s seconds", (i + 1), topic, RETRY_INTERVAL_ARR[i]));
												Thread.sleep(RETRY_INTERVAL_ARR[i] * 1000);
											}

											if (msgHandler.useByteArray()) {
												// if not override onMsg(String msgId, byte[] msg), maybe a null invoke.
												msgHandler.onMsg(new String(messageAndMetadata.key(), "UTF-8"), messageAndMetadata.message());
											} else {
												msgHandler.onMsg(new String(messageAndMetadata.key(), "UTF-8"), new String(messageAndMetadata.message(), "UTF-8"));
											}
										} catch (Exception e1) {
											LOG.error(String.format("Retrying %s times met errors for consuming topic:%s", (i+1), topic), e1);
										}
									}
								} else {
									LOG.error(String.format(Thread.currentThread().getName()+"->[NO RETRY] Processing msg:[%s] met error, please check it.", new String(messageAndMetadata.message(), "UTF-8")), ee);
								}
							}

							msgHandler.increment();

							// should commit offset or not
							if (msgHandler.shouldCommit()) {
								consumer.commitOffsets();
								msgHandler.setLastTimeCommit(System
										.currentTimeMillis());
								if (LOG.isInfoEnabled()) {
									LOG.info(Thread.currentThread().getName()+"-> Successfully commitOffsets, topic:"
											+ messageAndMetadata.topic()
											+ " partition: "
											+ messageAndMetadata.partition()
											+ " offset: "
											+ messageAndMetadata.offset());

								}
							}

						}
					} catch (Exception e) {
						LOG.error(e.getMessage(), e);
						try {
							if (messageAndMetadata != null) {
								LOG.error("topic:" + messageAndMetadata.topic() +
										" partition:", messageAndMetadata.partition() +
										" msgContent:" + new String(messageAndMetadata.message(), "UTF-8"), e);
							}
						} catch (UnsupportedEncodingException e1) {
							LOG.error(e1.getMessage(), e1);
						}
					} finally {
						LOG.info(String.format("KafkaConsumer thread for topic:%s will give up, try me later...", topic));
						if (consumer != null) {
							consumer.shutdown();
						}
					}

					}
				});
			consumerThread.setName("KafkaConsumerThread"+i+"-"+ properties.getProperty("topic"));
			consumerThread.start();
		}
		LOG.info("init KafkaConsumerListener end...[topic:" + topic + "]");

	}*/


    public void init() {
        LOG.info(topicList.size() + " topic initialization...");
        for (String topic : topicList) {
            final String fintopic = topic; //properties.getProperty("topic");
            this.properties.put("topic", topic);
            final int consumerSize = parseConsumerSize(properties);
            ConsumerConfig config = new ConsumerConfig(properties);

            LOG.info("init KafkaConsumerListener starting...  [topic:" + fintopic + "]");
            for (int i = 0; i < consumerSize; i++) {
                LOG.info("thred|init KafkaConsumerListener...");
                final KafkaConsumer consumer = new KafkaConsumer(fintopic, config);
                final KafkaMessageListener msgHandler = (KafkaMessageListener) messageHandler.clone();
                final boolean isUniformRetry = msgHandler.isUseUniformRetry();
                final long uniformRetryMs = msgHandler.getUniformIntervalMs();

                if (msgHandler == null) {
                    throw new KafkaException("Can not build messageHandler for the topic:" + fintopic);
                }
                msgHandler.setTopic(fintopic);
                Thread consumerThread = new Thread(new Runnable() {

                    public void run() {
                        MessageAndMetadata<byte[], byte[]> messageAndMetadata = null;
                        try {
                            while (consumer.hasNext() && !Thread.interrupted()) {
                                messageAndMetadata = consumer.getData();

                                String messageId = new String(messageAndMetadata.key()==null?String.valueOf(messageAndMetadata.hashCode()).getBytes():messageAndMetadata.key(), "UTF-8");

                                try {
                                    if (msgHandler.useByteArray()) {
                                        // if not override onMsg(String msgId, byte[] msg), maybe a null invoke.
                                        msgHandler.onMsg(messageId, messageAndMetadata.message());
                                    } else {
                                        msgHandler.onMsg(messageId, new String(messageAndMetadata.message(), "UTF-8"));
                                    }

                                } catch (Exception ee) {
                                    LOG.error(ee.getMessage(), ee);
                                    long retries = msgHandler.getErrorMaxRetries();
                                    // range from 1 ~ DEFAULT_MAX_RETRIES do as it, other retry DEFAULT_MAX_RETRIES times
                                    if (retries != -1) {
                                        if (retries <= 0 || retries > DEFAULT_MAX_RETRIES) {
                                            retries = DEFAULT_MAX_RETRIES;
                                        }

                                        for (int i = 0; i < retries; i++) {
                                            try {
                                                if (isUniformRetry) {
                                                    LOG.info(String.format(Thread.currentThread().getName() + "->[USING RETRY] %s times, for consuming topic:%s, sleeping %s seconds", (i + 1), fintopic, (uniformRetryMs / 1000)));
                                                    Thread.sleep(uniformRetryMs);
                                                } else {
                                                    LOG.info(String.format(Thread.currentThread().getName() + "->[USING RETRY] %s times, for consuming topic:%s, sleeping %s seconds", (i + 1), fintopic, RETRY_INTERVAL_ARR[i]));
                                                    Thread.sleep(RETRY_INTERVAL_ARR[i] * 1000);
                                                }

                                                if (msgHandler.useByteArray()) {
                                                    // if not override onMsg(String msgId, byte[] msg), maybe a null invoke.
                                                    msgHandler.onMsg(messageId, messageAndMetadata.message());
                                                } else {
                                                    msgHandler.onMsg(messageId, new String(messageAndMetadata.message(), "UTF-8"));
                                                }
                                            } catch (Exception e1) {
                                                LOG.error(String.format("Retrying %s times met errors for consuming topic:%s", (i + 1), fintopic), e1);
                                            }
                                        }
                                    } else {
                                        LOG.error(String.format(Thread.currentThread().getName() + "->[NO RETRY] Processing msg:[%s] met error, please check it.", new String(messageAndMetadata.message(), "UTF-8")), ee);
                                    }
                                }

                                msgHandler.increment();

                                // should commit offset or not
                                if (msgHandler.shouldCommit()) {
                                    consumer.commitOffsets();
                                    msgHandler.setLastTimeCommit(System.currentTimeMillis());
                                    if (LOG.isInfoEnabled()) {
                                        LOG.info(Thread.currentThread().getName() + "-> Successfully commitOffsets, topic:"
                                                + messageAndMetadata.topic()
                                                + " partition: "
                                                + messageAndMetadata.partition()
                                                + " offset: "
                                                + messageAndMetadata.offset());

                                    }
                                }

                            }
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                            try {
                                if (messageAndMetadata != null) {
                                    LOG.error("topic:" + messageAndMetadata.topic() +
                                            " partition:", messageAndMetadata.partition() +
                                            " msgContent:" + new String(messageAndMetadata.message(), "UTF-8"), e);
                                }
                            } catch (UnsupportedEncodingException e1) {
                                LOG.error(e1.getMessage(), e1);
                            }
                        } finally {
                            LOG.info(String.format("KafkaConsumer thread for topic:%s will give up, try me later...", fintopic));
                            if (consumer != null) {
                                consumer.shutdown();
                            }
                        }

                    }
                });
                consumerThread.setName("KafkaConsumerThread" + i + "-" + properties.getProperty("topic"));
                consumerThread.start();
            }
            LOG.info("init KafkaConsumerListener end...[topic:" + topic + "]");
        }
    }

    private int parseConsumerSize(Properties properties) {
        int consumerSize;
        try {
            consumerSize = Integer.valueOf(properties.getProperty("consumer.size").trim());
        } catch (Exception e) {
            consumerSize = DEFAULT_MIN_CONSUMER_NUM;
        }
        if (consumerSize < DEFAULT_MIN_CONSUMER_NUM || consumerSize > DEFUALT_MAX_CONSUMSER_NUM) {
            consumerSize = DEFAULT_MIN_CONSUMER_NUM;
        }
        LOG.info("USing consumerSize: {}.", consumerSize);
        return consumerSize;
    }

    public KafkaMessageListener getMessageHandler() {
        return messageHandler;
    }

    public void setMessageHandler(KafkaMessageListener messageHandler) {
        this.messageHandler = messageHandler;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setConflis(List<Properties> conflis) {
        this.conflis = conflis;
    }

    public void setTopics(String topics) {
        this.topics = topics;
        this.topicList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(topics);
    }

    public void close() {
        if (consumers != null) {
            for (KafkaConsumer consumer : consumers) {
                consumer.shutdown();
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        if (consumers != null) {
            for (KafkaConsumer consumer : consumers) {
                consumer.shutdown();
            }
        }
    }
}