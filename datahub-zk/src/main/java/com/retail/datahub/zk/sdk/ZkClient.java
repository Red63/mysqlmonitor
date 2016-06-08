package com.retail.datahub.zk.sdk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.zk.sdk.ZkClient <br/>
 * date: 2016/5/18 11:13 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ZkClient {

    private String zkLists;

    private String defaultNode;

    private int retry = 3;

    private int baseSleepTimeMs = 1000;

    CuratorFramework client;

    public void setZkLists(String zkLists) {
        this.zkLists = zkLists;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public void setDefaultNode(String defaultNode) {
        this.defaultNode = defaultNode;
    }

    public ZkClient() {
    }


    public ZkClient(String zkLists) {
        this.zkLists = zkLists;
    }

    public ZkClient(String zkLists, int retry) {
        this.zkLists = zkLists;
        this.retry = retry;
    }

    private void start() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, retry);
        //client = CuratorFrameworkFactory.newClient(zkLists, retryPolicy);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkLists)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace(defaultNode == null ? "" : defaultNode)
                .build();
        client.start();
    }

    private void close() {
        if (client != null) client.close();
    }

    public String getNode(String path) throws Exception {
        try {
            start();
            return new String(client.getData().forPath(path));
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public byte[] getData(String path) throws Exception{
        try {
            start();
            return client.getData().forPath(path);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public List<String> watchedGetChildren(String path) throws Exception {
        try {
            start();
            return client.getChildren().watched().forPath(path);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }

    public void updateNode(String path, String data) throws Exception {
        try {
            start();
            client.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public void deleteNode(String path) throws Exception {
        try {
            start();
            client.delete().forPath(path);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public void deleteChildrenNode(String path) throws Exception {
        try {
            start();
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public void deleteKafkaTopic(String topic) throws Exception {
        deleteChildrenNode("/brokers/topics/" + topic);
    }

    public void deleteKafkaConsumerGroup(String group) throws Exception {
        deleteChildrenNode("/consumers/" + group);
    }
}
