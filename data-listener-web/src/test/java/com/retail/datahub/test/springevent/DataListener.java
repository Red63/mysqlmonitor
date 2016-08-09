package com.retail.datahub.test.springevent;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.DataListener <br/>
 * date: 2016/7/28 14:50 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
@Component
public class DataListener implements ApplicationListener<CountEvent> {
    @Override
    public void onApplicationEvent(CountEvent event) {
        System.out.println(200 + (Integer) event.getSource());
    }
}
