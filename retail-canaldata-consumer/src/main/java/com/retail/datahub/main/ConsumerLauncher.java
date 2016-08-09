package com.retail.datahub.main;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.retail.commons.dkafka.product.DefaultEventProduct;
import com.retail.datahub.base.BaseCanalClient;
import com.retail.datahub.base.ResouseConstants;
import com.retail.datahub.exception.ProcessDataException;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.main.ConsumerLauncher <br/>
 * date: 2016/4/28 17:02 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ConsumerLauncher extends BaseCanalClient{
    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(ConsumerLauncher.class);
    private static final Logger errorLogger          = LoggerFactory.getLogger("msg-error");
    private static final Logger binlogLogger         = LoggerFactory.getLogger("binlog-msg");
    private static final Logger heartbeatLogger      = LoggerFactory.getLogger("heartbeat-msg");

    private String zkList;

    private String topic_prefix;
    private String topic_suffix;

    private DefaultEventProduct mysqlLogProduct;

    //用于对比
    private static String upJson = "";

    public static void main(String[] args) throws Throwable{
        try {
            String conf = System.getProperty("listener.conf", "classpath:listener.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(ConsumerLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }



            logger.info("## start the retail-canaldata-consumer server.");

            final ConsumerLauncher consumerLauncher = new ConsumerLauncher();
            consumerLauncher.readResouse(properties);
            CanalConnector connector = CanalConnectors.newClusterConnector(consumerLauncher.zkList, consumerLauncher.destination, "", "");
            consumerLauncher.setConnector(connector);
            consumerLauncher.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the retail-canaldata-consumer client");
                        consumerLauncher.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        logger.info("## retail-canaldata-consumer client is down.");
                    }
                }

            });

            logger.info("## the retail-canaldata-consumer server is running now ......");
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the retail-canaldata-consumer Server:\n{}",
                    ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }

    private void readResouse(Properties properties){
        this.listenerdb = getProperty(properties, ResouseConstants.LISTENER_DB);
        this.listenertable = getProperty(properties, ResouseConstants.LISTENER_TABLE);
        this.event = getProperty(properties, ResouseConstants.EVENT);
        this.destination = getProperty(properties,ResouseConstants.DESTINATION);
        this.username = getProperty(properties, ResouseConstants.USERNAME);
        this.password = getProperty(properties, ResouseConstants.PASSWORD);
        this.zkList = StringUtils.isEmpty(getProperty(properties, ResouseConstants.ZK_LIST))?"127.0.0.1:2181":getProperty(properties, ResouseConstants.ZK_LIST);
        this.topic_prefix = getProperty(properties, ResouseConstants.TOPIC_PREFIX);
        this.topic_suffix = getProperty(properties, ResouseConstants.TOPIC_SUFFIX);

        try {
            logger.info("init kafka producerListener...");

            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"classpath*:spring/spring-config-kafka-producer.xml"});
            context.start();

            mysqlLogProduct = (DefaultEventProduct) context.getBean("mysqlLogProduct");
        } catch (Exception e){
            logger.error("init kafka producerListener error...", e);
        }
    }

    private String getProperty(Properties properties, String key) {
        return StringUtils.trim(properties.getProperty(StringUtils.trim(key)));
    }

    @Override
    public void processData(List<EventBatchModel> eventBatchModels) throws ProcessDataException {
        //TODO

        for(EventBatchModel eventBatchModel : eventBatchModels){
            String msg = "";
            String topic;
            if(StringUtils.isNotEmpty(topic_prefix)){
                topic = topic_prefix + "_" + eventBatchModel.getDbName();
            } else {
                topic = "binlog_" + eventBatchModel.getDbName();
            }

            topic = StringUtils.isNotEmpty(topic_suffix)?topic + "_" + topic_suffix:topic;


            try {
                msg = JSONObject.toJSONString(eventBatchModel, SerializerFeature.WriteMapNullValue);

                //检查msg数据是否重复,如果重复不处理
                if(!StringUtils.equals(msg, upJson)){
                    //心跳表另外处理
                    if(StringUtils.equals("heartbeat", eventBatchModel.getRealTableName())) {
                        heartbeatLogger.info(String.format("心跳表:%s.%s|%s", eventBatchModel.getDbName(),
                                eventBatchModel.getRealTableName(),
                                JSONObject.toJSONString(eventBatchModel.getRowData()[0].getAfterColumns())));
                    } else {
                        mysqlLogProduct.send(topic, msg);
                        binlogLogger.info(String.format("topic:%s,size:%d,msg:%s", topic, eventBatchModels.size(), msg));
                    }
                }
                upJson = msg;
            } catch (Exception e){
                upJson = "";
                errorLogger.error(String.format("send msg to topic [%s] error...", topic), e);
                errorLogger.info(String.format("unsent msg:%s", msg));

                throw new ProcessDataException(e);
            }

        }
    }
}
