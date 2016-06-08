package com.retail.datahub.kafka.sdk.consumer;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class KafkaMessageListener implements Cloneable {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageListener.class);

	private String topic;
	private volatile long lastTimeCommit = System.currentTimeMillis();
	private AtomicLong counter = new AtomicLong(1L);
	private long msgCntShouldCommit = 1;
	private long timeOutShouldCommit;
	private long errorMaxRetries = -1;
	private boolean useUniformRetry = false;
	private long uniformIntervalMs = 10000;

	public boolean isUseUniformRetry() {
		return useUniformRetry;
	}

	public void setUseUniformRetry(boolean useUniformRetry) {
		this.useUniformRetry = useUniformRetry;
	}

	public long getUniformIntervalMs() {
		return uniformIntervalMs;
	}

	public void setUniformIntervalMs(long uniformIntervalMs) {
		if (uniformIntervalMs <= 0) {
			LOG.warn("The property uniformIntervalMs must be a positive value, " +
					"but you reset it as " + uniformIntervalMs);
			this.uniformIntervalMs = 10000;
		} else {
			this.uniformIntervalMs = uniformIntervalMs;
		}
	}

	public void setCounter(AtomicLong counter) {
		this.counter = counter;
	}

	public long getErrorMaxRetries() {
		return errorMaxRetries;
	}

	public void setErrorMaxRetries(long errorMaxRetries) {
		this.errorMaxRetries = errorMaxRetries;
	}

	protected final void increment() {
		counter.getAndIncrement();
	}

	/**
	 * Don't override this method when Common Usage Condition
	 *
	 * @return
	 */
	protected boolean shouldCommit() {
		boolean retVal = false;
		long now = System.currentTimeMillis();
		long timeSpan = now - getLastTimeCommit();

		if (counter.longValue() % msgCntShouldCommit == 0 || timeSpan >= timeOutShouldCommit) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("counter:" + counter.longValue() + " timeSpan:" + timeSpan + " topic: " + topic);
			}
			retVal = true;
		}
		return retVal;
	}

	public long getMsgCntShouldCommit() {
		return msgCntShouldCommit;
	}

	public void setMsgCntShouldCommit(long msgCntShouldCommit) {
		if (msgCntShouldCommit <= 0) {
			LOG.warn("The property msgCntShouldCommit must be a positive value, " +
					"but you reset it as " + msgCntShouldCommit);
			this.msgCntShouldCommit = 1;
		} else {
			this.msgCntShouldCommit = msgCntShouldCommit;
		}
	}

	public long getTimeOutShouldCommit() {
		return timeOutShouldCommit;
	}

	public void setTimeOutShouldCommit(long timeOutShouldCommit) {
		this.timeOutShouldCommit = timeOutShouldCommit;
	}

	protected abstract void onMsg(String msgId, String message);

	protected void onMsg(String msgId, byte[] message) {
	}

	protected boolean useByteArray() {
		return false;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getLastTimeCommit() {
		return lastTimeCommit;
	}

	public void setLastTimeCommit(long lastTimeCommit) {
		this.lastTimeCommit = lastTimeCommit;
	}

	//  should deeply clone!!!
	public Object clone() {
		KafkaMessageListener listener = null;
		try {
			listener = (KafkaMessageListener) super.clone();
		} catch (CloneNotSupportedException e) {
			LOG.error(e.getMessage(), e);
		}
		listener.setLastTimeCommit(System.currentTimeMillis());
		listener.setCounter(new AtomicLong(1L));
		return listener;
	}
}