package com.retail.datahub.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import com.retail.datahub.base.BaseCanalClient;
import com.retail.datahub.exception.ProcessDataException;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.List;

/**
 * 集群模式
 *
 * Created by red on 2016/3/11.
 */
public abstract class ClusterCanalClient extends BaseCanalClient {

    private String zkserver;

    public ClusterCanalClient(){}

    public ClusterCanalClient(String destination){
        super(destination);
    }

    public void setZkserver(String zkserver) {
        this.zkserver = zkserver;
    }

    public void init() {
        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector(this.zkserver, this.destination, this.username, this.password);

        //final ClusterCanalClient clientTest = new ClusterCanalClient(destination);
        final ClusterCanalClient _this = this;
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
