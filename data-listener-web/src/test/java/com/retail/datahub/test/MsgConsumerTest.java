package com.retail.datahub.test;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.MsgConsumerTest <br/>
 * date: 2016/5/3 10:47 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class MsgConsumerTest {

    public static void main(String[] args){
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"classpath*:conf/spring-config-kafka-consumer.xml"});
        context.start();
    }
}
