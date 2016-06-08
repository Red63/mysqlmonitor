package com.retail.data.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.retail.commons.dkafka.EventProcesser;
import com.retail.commons.dkafka.exception.EventProcesserException;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import com.retail.datahub.model.common.ModelConvert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.data.listener.MsgConsumer <br/>
 * date: 2016/5/3 10:18 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class MsgConsumer implements EventProcesser {

    private static final Logger logger = LoggerFactory.getLogger(MsgConsumer.class);

    @Override
    public Object processes(Object data) throws EventProcesserException {
        //System.out.println("消息获取:" + msgId + "|msg:" + message);

        System.out.println(data);

        /*EventBatchModel eventBatchModel = JSON.parseObject(String.valueOf(data), EventBatchModel.class);

        System.out.println(Thread.currentThread().getName() + "["+ Thread.currentThread().getId() +"]|es do..." + eventBatchModel.getDbName() + ":" + eventBatchModel.getRealTableName() + ",batchId:" + eventBatchModel.getBatchId() + ",eventType:" + eventBatchModel.getEventType());

        String json = JSONObject.toJSONString(ModelConvert.convert(eventBatchModel), SerializerFeature.WriteMapNullValue);

        System.out.println("json:" + json);*/

        /*Response response = client.index(eventBatchModel.getDbName(), eventBatchModel.getRealTableName(), json);

        System.out.println("es:" + response);*/

        return data;
    }
}
