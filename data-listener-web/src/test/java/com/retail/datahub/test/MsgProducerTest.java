package com.retail.datahub.test;

import com.retail.commons.dkafka.exception.EncodeException;
import com.retail.commons.dkafka.exception.EventProcesserException;
import com.retail.commons.dkafka.product.DefaultEventProduct;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.KafkaTest <br/>
 * date: 2016/4/29 19:00 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class MsgProducerTest {

    public static void main(String[] args){
        /*Properties properties = new Properties();
        properties.put("metadata.broker.list", "10.13.3.10:9092,10.13.3.12:9092");
        //properties.put("serializer.class", "kafka.serializer.DefaultEncoder");

        KafkaProducerListener producerListener = new KafkaProducerListener(properties);

        producerListener.send("test_canal", "test sdk message");*/

        DefaultEventProduct mysqlLogProduct = getBean("mysqlLogProduct");

        try {
            mysqlLogProduct.send("test", "test sdk message 7");
        } catch (EncodeException e) {
            e.printStackTrace();
        } catch (EventProcesserException e) {
            e.printStackTrace();
        }

    }

    private static <T> T getBean(String beaName){
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"classpath*:conf/spring-config-kafka-producer.xml"});
        context.start();

        T bean = (T) context.getBean(beaName);

        return bean;
    }

}
