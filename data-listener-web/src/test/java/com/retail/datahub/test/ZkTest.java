package com.retail.datahub.test;

import com.alibaba.otter.canal.client.impl.running.ClientRunningData;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.retail.datahub.zk.sdk.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.ZkTest <br/>
 * date: 2016/4/29 18:50 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ZkTest {


    @Test
    public void oldzk() throws Exception {
        ZooKeeper zk = new ZooKeeper("10.13.3.10:2181", 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // TODO Auto-generated method stub
                System.out.println("已经触发了" + event.getType() + "事件！");
            }
        });


        // 取出子目录节点列表
        System.out.println(zk.getChildren("/", true));

        System.out.println(zk.getChildren("/brokers/topics", true));

        //zk.delete("/brokers/topics/topic_rtl_contract", -1);

        /*for(int i=0; i<6; i++){
            zk.delete("/brokers/topics/topic_rtl_contract/" + i + "/", -1);
        }*/
    }

    @Test
    public void getData(){
        ZkClient client = getBean("zkClient");

        try {

            //System.out.println(client.watchedGetChildren("/otter/canal/cluster"));

            System.out.println(client.watchedGetChildren("/otter/canal/destinations/loctest/1001"));


            byte[] bytes = client.getData("/otter/canal/destinations/loctest/1001/cursor");

            System.out.println(new String(bytes));

            ClientRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);

            System.out.println(eventData.getAddress());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteDes(){

        ZkClient client = getBean("zkClient");

        try {
            client.deleteChildrenNode("/otter/canal/destinations/cxmonitor");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delGroup(){
        ZkClient client = getBean("zkClient");

        for (int i=1;i<=1;i++){


            try {
                client.deleteKafkaConsumerGroup("test_canal");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }



    }

    @Test
    public void delTopic(){
        ZkClient client = getBean("zkClient");

        try {
            client.deleteKafkaTopic("datahubrtl_contract");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void update(){
        ZkClient client = getBean("zkClient");
        try {
            String json = "{\"@type\":\"com.alibaba.otter.canal.protocol.position.LogPosition\",\"identity\":{\"slaveId\":-1,\"sourceAddress\":{\"address\":\"10.13.2.64\",\"port\":3306}},\"postion\":{\"included\":false,\"journalName\":\"mysql-bin.000003\",\"position\":0,\"timestamp\":1464229332000}}";
            client.updateNode("/otter/canal/destinations/loctest/1001/cursor", json);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private <T> T getBean(String beaName){
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"classpath*:conf/spring-config-zk.xml"});
        context.start();

        T bean = (T) context.getBean(beaName);

        return bean;
    }

}
