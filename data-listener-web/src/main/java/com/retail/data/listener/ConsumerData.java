package com.retail.data.listener;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.retail.datahub.es.model.Response;
import com.retail.datahub.es.sdk.EsClient;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import com.retail.datahub.canal.ClusterCanalClient;
import com.retail.datahub.exception.ProcessDataException;
import com.retail.datahub.model.common.ModelConvert;

import javax.annotation.Resource;
import java.util.List;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.data.listener.ConsumerData <br/>
 * date: 2016/4/27 19:42 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ConsumerData extends ClusterCanalClient{
    @Resource
    private EsClient client;

    @Override
    public void processData(List<EventBatchModel> eventBatchModels) throws ProcessDataException {

        for (EventBatchModel eventBatchModel:eventBatchModels){
            System.out.println("es do...|" + eventBatchModel.getDbName() + ":" + eventBatchModel.getRealTableName() + ",batchId:" + eventBatchModel.getBatchId() + ",eventType:" + eventBatchModel.getEventType());

            String json = JSONObject.toJSONString(ModelConvert.convert(eventBatchModel), SerializerFeature.WriteMapNullValue);

            System.out.println("json:" + json);

            Response response = client.index(eventBatchModel.getDbName(), eventBatchModel.getRealTableName(), json);

            System.out.println("es:" + response);
        }

    }
}
