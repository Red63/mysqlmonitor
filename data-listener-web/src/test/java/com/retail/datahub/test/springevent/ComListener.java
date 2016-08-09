package com.retail.datahub.test.springevent;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.ComListener <br/>
 * date: 2016/7/28 17:15 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
@Component
public class ComListener implements ApplicationListener {

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if(event instanceof MessageEvent){

        } else {
            System.out.println(event.getSource());
        }

    }
}
