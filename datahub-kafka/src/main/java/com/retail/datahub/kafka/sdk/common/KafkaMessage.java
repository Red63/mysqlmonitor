package com.retail.datahub.kafka.sdk.common;


import java.util.HashMap;
import java.util.Map;

public class KafkaMessage {
	private String key;
	private byte[] msg;
	private Map<String, Object> header;
	
	public KafkaMessage(byte[] msg) {
		this.key = null;
		this.msg = msg;
	}
	
	public KafkaMessage(String key, byte[] msg) {
		this.key = key;
		this.msg = msg;
		this.header = new HashMap<>();
	}

	public void addHeader(String name, Object val) {
		this.header.put(name, val);
	}

	public void delHeader(String name) {
		this.header.remove(name);
	}
	
	public String getKey() {
		return this.key;
	}
	
	public byte[] getMsg() {
		return this.msg;
	}
}
