package com.retail.datahub.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import com.retail.datahub.base.BaseCanalClient;
import com.retail.datahub.exception.ProcessDataException;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * 单机模式
 * 
 * Created by red on 2016/3/11.
 */
public abstract class SimpleCanalClient extends BaseCanalClient {


    public SimpleCanalClient(){}

    public SimpleCanalClient(String des){
        super(des);
    }

    public void init() {
        //logger.info("服务器:" + this.canalhost);
        // 根据ip，直接创建链接，无HA的功能
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(this.canalhost,
            this.port), this.destination, this.username, this.password);

        //final SimpleCanalClient clientTest = new SimpleCanalClient(destination);
        final SimpleCanalClient _this = this;
        _this.setConnector(connector);
        _this.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    _this.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }

    @Override
    public abstract void processData(List<EventBatchModel> eventBatchModels) throws ProcessDataException;

}
