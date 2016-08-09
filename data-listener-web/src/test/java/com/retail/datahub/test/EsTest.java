package com.retail.datahub.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.retail.datahub.es.model.Response;
import com.retail.datahub.es.model.SqlResponse;
import com.retail.datahub.es.sdk.EsClient;
import com.retail.datahub.es.util.DateUtil;
import com.retail.datahub.test.domian.User;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.junit.Test;
import org.nlpcn.es4sql.domain.SearchResult;
import org.nlpcn.es4sql.domain.Select;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.annotation.Resource;
import java.sql.SQLFeatureNotSupportedException;
import java.text.ParseException;
import java.util.*;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.EsTest <br/>
 * date: 2016/5/10 17:24 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class EsTest {

    @Test
    public void test(){
        EsClient client = getBean("esClient");

        List<String> index = new ArrayList<>();
        List<String> type = new ArrayList<>();

        index.add("rtl_contract");
        type.add("CONTRACT_EXECUTE");

        Response response  = client.multiMatchSearch(index, type, "afterColumns", "123456");//client.search(index, type, "*5111*");//

        //System.out.println(response.toString());

        //response.getHits().getHits()[0].getSource();
        System.out.println(JSONObject.toJSONString(response, true));


        /*****/
        /*Map<String, Object> map = new HashMap<>();
        map.put("eventData", new Date());
        map.put("name", "date1");
        client.index("my_test", "date_test", JSONObject.toJSONString(map));*/
    }

    @Test
    public void mapIndex() throws Exception {
        EsClient client = getBean("esClient");
        /*User user = new User();
        user.setId(2l);
        user.setName("李四");
        user.setCode("001002");
        user.setAge(25);
        user.setModifyCreate(new Date());

        Response response = client.index("my_test", "user", String.valueOf(user.getId()), user);

        System.out.println(JSONObject.toJSONString(response, true));*/

        Map<String, Object> book = new HashMap<>();
        book.put("name", "es基础");
        book.put("id", 5);
        book.put("price", "56.3");

        client.index("my_test", "book", String.valueOf(book.get("id")), book);
    }

    @Test
    public void objIndex() throws Exception {
        EsClient client = getBean("esClient");

        User user = new User();
        user.setId(2l);
        user.setName("李四");
        user.setCode("001002");
        user.setAge(25);
        user.setModifyCreate(new Date());

        Response response = client.index("my_test", "user", String.valueOf(user.getId()), user);

        System.out.println(JSONObject.toJSONString(response, true));


        client.index("my_test", "user", String.valueOf(user.getId()), user);
    }


    @Test
    public void batchObjIndex(){
        EsClient client = getBean("esClient");

        List<User> users = new ArrayList<>();

        for(int i=0; i<100; i++){
            User user = new User();
            user.setId(Long.valueOf(i+1));
            user.setName("李四" + i);
            user.setCode("00100" + i);
            user.setAge(i + 1);
            user.setModifyCreate(new Date());

            users.add(user);
        }

        try {
            client.batchIndexByRouting("my_test", "user", users, false, "id");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void update(){
        EsClient client = getBean("esClient");

        Map<String, Object> data = new HashMap<>();
        data.put("modifyCreate", null);

        try {
            client.updateOne("my_test", "user", "1", data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void objUpdate(){
        EsClient client = getBean("esClient");

        User user = new User();
        user.setId(2l);
        user.setName("张三三");
        user.setModifyCreate(new Date());

        try {
            client.updateOne("my_test", "user", user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T> T getBean(String beaName){
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"classpath*:conf/spring-config-es.xml"});
        context.start();

        T bean = (T) context.getBean(beaName);

        return bean;
    }

    @Test
    public void testRange(){
        EsClient client = getBean("esClient");

        List<String> index = new ArrayList<>();
        List<String> type = new ArrayList<>();

        index.add("rtl_contract");
        //index.add("rtl_customer");

        //type.add("cus_customer");

        /*Date start = new Date(1463117335000L);
        Date end = new Date(1463117335000L);*/

        Date start = null;
        Date end = null;
        try {
            start = DateUtil.parse("2016-05-04 00:00:00", DateUtil.DATE_TIME_FORMAT);
            end = DateUtil.parse("2016-05-06 24:00:00", DateUtil.DATE_TIME_FORMAT);
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {

        }


        Response response = client.dateRangeForValueSearch(index, type, "暗", start, end);

        //Response response = client.dateRangeSearch(index, type, start, end);
        System.out.println(JSONObject.toJSONString(response, true));
    }

    @Test
    public void batchIndex(){
        EsClient client = getBean("esClient");

        List<String> jsons = new ArrayList<>();

        jsons.add("{ \"batchId\": 201,\"dbName\": \"rtl_customer\",\"eventDate\": \"2016-05-04 17:26:11\",\"eventType\": \"UPDATE\",\"executionTime\": 1462340849000,\"logicTableName\": \"cus_card\",\"realTableName\": \"cus_card\",\"rowData\": [ { \"afterColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"160504130407960870\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"},\"beforeColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"}}]}");
        jsons.add("{ \"batchId\": 201,\"dbName\": \"rtl_customer\",\"eventDate\": \"2016-05-04 17:26:11\",\"eventType\": \"UPDATE\",\"executionTime\": 1462340849000,\"logicTableName\": \"cus_card\",\"realTableName\": \"cus_card\",\"rowData\": [ { \"afterColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"160504130407960870\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"},\"beforeColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"}}]}");
        jsons.add("{ \"batchId\": 201,\"dbName\": \"rtl_customer\",\"eventDate\": \"2016-05-04 17:26:11\",\"eventType\": \"UPDATE\",\"executionTime\": 1462340849000,\"logicTableName\": \"cus_card\",\"realTableName\": \"cus_card\",\"rowData\": [ { \"afterColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"160504130407960870\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"},\"beforeColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"}}]}");
        jsons.add("{ \"batchId\": 201,\"dbName\": \"rtl_customer\",\"eventDate\": \"2016-05-04 17:26:11\",\"eventType\": \"UPDATE\",\"executionTime\": 1462340849000,\"logicTableName\": \"cus_card\",\"realTableName\": \"cus_card\",\"rowData\": [ { \"afterColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"160504130407960870\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"},\"beforeColumns\": { \"area_id\": \"10\",\"business_status\": \"\",\"change_balance\": \"0.0\",\"comment\": \"\",\"create_date\": \"2016-05-04 13:47:29\",\"create_relation_id\": \"\",\"create_relation_type\": \"\",\"cus_card_type\": \"0\",\"cus_code\": \"\",\"cus_disount\": \"100.0\",\"cus_format\": \"1\",\"cus_id\": \"6169\",\"cus_level\": \"1\",\"cus_out_card_code\": \"\",\"cus_trade\": \"\",\"head_url\": \"\",\"id\": \"12623\",\"integral\": \"0\",\"login_password\": \"\",\"modify_date\": \"2016-05-04 13:47:29\",\"modify_relation_id\": \"\",\"modify_relation_type\": \"\",\"nike_show_name\": \"\",\"pay_password\": \"\",\"rebate_balance\": \"0.0\",\"register_channel\": \"\",\"register_platform\": \"\",\"register_way\": \"\",\"shop_id\": \"\",\"status\": \"1\",\"total_balance\": \"0.0\"}}]}");

        BulkResponse response = client.batchIndex("rtl_customer", "cus_card", jsons);

        System.out.println(response.getItems());
    }

    @Test
    public void whereSerach(){
        EsClient client = getBean("esClient");
        Map<String, Object> where = new HashMap<>();
        where.put("id", 3);

        Response response = client.whereForSearch("my_test","user",where);

        System.out.println(JSONObject.toJSONString(response, true));
    }

    @Test
    public void queryBySql(){
        EsClient client = getBean("esClient");

        String sql = "select sum(age) as age from my_test/user group by id,code";

        try {
            SqlResponse sqlResponse = client.asSql().select(sql);

            Aggregations result = sqlResponse.getSearchResponse().getAggregations();

            System.out.println(sqlResponse.getSearchResponse().toString());

        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void explainSql(){
        EsClient client = getBean("esClient");

        String sql = "SELECT count(uniqueCode) FROM stock_test_import/goods_20160622 group by uniqueCode HAVING count(uniqueCode) >3";

        try {

            String dsl = client.asSql().explain(sql);

            System.out.println(dsl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void showIndex(){
        EsClient client = getBean("esClient");

        try {

            System.out.println(client.asSql().exist("my_test/user"));
        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        }
    }


    @Test
     public void queryByBaseSql(){
        EsClient client = getBean("esClient");

        String sql = "select * from my_test/book";

        try {

            SqlResponse response = client.asSql().select(sql);


            List<User> results = response.hitsResult(User.class);

            System.out.println(results.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void queryAggBaseSql(){
        EsClient client = getBean("esClient");

        //String sql = "SELECT stats(promotionPrice) as count FROM stock_test_import/goods_20160622 group by uniqueCode limit 2";

        String sql = "SELECT extended_stats(promotionPrice) as count FROM stock_test/20160622 group by uniqueCode limit 2";

        //String sql = "SELECT count(uniqueCode) as count,max(promotionPrice) FROM stock_test_import/goods_20160622 group by uniqueCode limit 2";

        try {

            Set<Map<String, Object>> results = client.asSql().select(sql).aggregationResult();
            for (Map rs : results){
                System.out.println(JSONObject.toJSONString(rs, SerializerFeature.WriteMapNullValue));
            }

            System.out.println("size():" + results.size());

            System.out.println(results);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void deleteIndex(){
        EsClient client = getBean("esClient");

        String delete = "delete from my_test/book where id='4'";

        try {
            client.asSql().delete(delete);
        } catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("over");
    }

    @Test
    public void scroll(){
        EsClient client = getBean("esClient");

        String scrollSql = "select /*! USE_SCROLL(1000,60000) */ * FROM stock_test";
        try {

            long start = System.currentTimeMillis();

            SqlResponse response = client.asSql().select(scrollSql);

            //System.out.println("第一次查询结果：" + response.getSearchResponse().getHits().getHits().length);

            String scrollId = response.getSearchResponse().getScrollId();

            int i = 1;

            while (true) {
                response = client.scroll(response.getSearchResponse().getScrollId(), 60000);
                //Break condition: No hits are returned
                if (response.getSearchResponse().getHits().getHits().length == 0) {
                    break;
                }



                System.out.println("第" + i +"次>>>"+response.getSearchResponse().getHits().getHits().length);

                i++;
            }

            long end = (System.currentTimeMillis() - start)/1000;

            System.out.println("耗时(s)：" + end);
        } catch (SqlParseException e) {
            e.printStackTrace();
        } catch (SQLFeatureNotSupportedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
