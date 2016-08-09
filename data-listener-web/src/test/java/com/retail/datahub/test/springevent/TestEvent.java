package com.retail.datahub.test.springevent;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.TestEvent <br/>
 * date: 2016/7/25 14:32 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring-test-config.xml"})
public class TestEvent {
    @Autowired
    ApplicationContext applicationContext;

    @Test
    public void eventTest() throws InterruptedException {
        applicationContext.publishEvent(new MessageEvent("你妹啊"));

        applicationContext.publishEvent(new CountEvent(2));
    }
}
