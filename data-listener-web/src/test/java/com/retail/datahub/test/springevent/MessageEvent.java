package com.retail.datahub.test.springevent;

import org.springframework.context.ApplicationEvent;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.MessageEvent <br/>
 * date: 2016/7/25 14:26 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class MessageEvent extends ApplicationEvent {

    public MessageEvent(String message) {
        super(message);
    }
}
