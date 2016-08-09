package com.retail.datahub.test.springevent;

import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.MyListener <br/>
 * date: 2016/7/25 14:28 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
@Component
public class MyListener implements ApplicationListener<MessageEvent> {

    //异步执行
    //@Async
    @Override
    public void onApplicationEvent(MessageEvent messageEvent) {
        System.out.println("My message:" + messageEvent.getSource());
    }
}
