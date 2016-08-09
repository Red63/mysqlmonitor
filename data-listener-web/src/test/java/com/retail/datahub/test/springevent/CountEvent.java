package com.retail.datahub.test.springevent;

import org.springframework.context.ApplicationEvent;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.springevent.CountEvent <br/>
 * date: 2016/7/28 14:48 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class CountEvent extends ApplicationEvent{
    public CountEvent(Integer source) {
        super(source);
    }
}
