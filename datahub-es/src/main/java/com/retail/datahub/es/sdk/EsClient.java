package com.retail.datahub.es.sdk;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.base.Splitter;
import com.retail.datahub.es.exception.EsOperationException;
import com.retail.datahub.es.model.Response;
import com.retail.datahub.es.model.SqlResponse;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.search.SearchHit;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.sdk.EsClient <br/>
 * date: 2016/5/9 13:42 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class EsClient {

    private static final Logger logger = LoggerFactory.getLogger(EsClient.class);

    private String eslist;

    private Boolean sniff = false;

    private String clusterName;

    private Client client;

    private SearchDao searchDao;

    public Client getClient() {
        return client;
    }

    public void setEslist(String eslist) {
        this.eslist = eslist;
    }

    public void setSniff(Boolean sniff) {
        this.sniff = sniff;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void init() {
        if (this.eslist == null || this.eslist.length() == 0) {
            logger.error("eslist is null...");
            return;
        }

        logger.info("init es cluster.name[" + clusterName + "] client...|" + eslist);

        List<String> clusterList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(this.eslist);

        List<TransportAddress> transportAddresses = new ArrayList<>();

        for (String cluster : clusterList) {
            List<String> host = Splitter.on(":").trimResults().omitEmptyStrings().splitToList(cluster);
            String ip = host.get(0);
            Integer port = Integer.valueOf(host.get(1));

            try {
                transportAddresses.add(new InetSocketTransportAddress(InetAddress.getByAddress(getIpByte(ip)), port == null ? 9300 : port));
            } catch (UnknownHostException e) {
                logger.error("init es client error...", e);
                return;
            }
        }

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();

        this.client = TransportClient.builder()
                .settings(settings)
                .addPlugin(DeleteByQueryPlugin.class)
                .build()
                .addTransportAddresses(transportAddresses.toArray(new TransportAddress[transportAddresses.size()]));

        logger.info("init es cluster.name[" + clusterName + "] client success...|" + eslist);
    }


    public Response index(String _index, String _type, String json) {
        IndexResponse indexResponse = client.prepareIndex(_index, _type).setSource(json).get();
        return translation(indexResponse);
    }

    public Response index(String _index, String _type, String _id, String json) {
        IndexResponse indexResponse = client.prepareIndex(_index, _type, _id).setSource(json).get();
        return translation(indexResponse);
    }

    public <T> Response index(String _index, String _type, String _id, T data) throws Exception {
        String json = JSONObject.toJSONString(data, SerializerFeature.WriteMapNullValue);

        IndexResponse indexResponse = client.prepareIndex(_index, _type, _id).setSource(json).get();
        return translation(indexResponse);
    }

    /**
     * 批量插入自动生成ID
     *
     * @param _index
     * @param _type
     * @param jsons
     * @return
     * @throws EsOperationException
     */
    public BulkResponse batchIndex(String _index, String _type, List<String> jsons) throws EsOperationException {
        if (jsons == null) throw new EsOperationException("jsons is null or empty...");

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (String json : jsons) {
            bulkRequest.add(client.prepareIndex(_index, _type).setSource(json));
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            throw new EsOperationException("batchIndex error," + bulkResponse.buildFailureMessage());
        }

        return bulkResponse;
    }

    /**
     * 批量插入传入数据对象中的ID字段
     *
     * @param _index
     * @param _type
     * @param data
     * @return
     * @throws EsOperationException
     */
    public <T> BulkResponse batchObjIndex(String _index, String _type, List<T> data) throws EsOperationException {
        if (data == null) throw new EsOperationException("data is null or empty...");

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (T tObj : data) {
            Class clazz = tObj.getClass();
            Object value;
            try {
                value = clazz.getDeclaredMethod("getId").invoke(tObj);
            } catch (Exception e) {
                throw new EsOperationException("调用" + clazz.getName() + "实例getId方法错误", e);
            }

            String _id = String.valueOf(value);
            String json = JSONObject.toJSONString(tObj, SerializerFeature.WriteMapNullValue);
            bulkRequest.add(client.prepareIndex(_index.toLowerCase(), _type.toLowerCase(), _id).setSource(json));
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            throw new EsOperationException("batchIndex error," + bulkResponse.buildFailureMessage());
        }

        return bulkResponse;
    }

    /**
     *
     * 更新数据
     */
    public UpdateResponse updateOne(String _index, String _type, String _id, Map<String, Object> data) throws Exception{
        if (_index == null) throw new EsOperationException("_index is null or empty...");
        if (_type == null) throw new EsOperationException("_type is null or empty...");
        if (_id == null) throw new EsOperationException("_id is null or empty...");
        if (data == null) throw new EsOperationException("data is null or empty...");

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(_index.toLowerCase()).type(_type.toLowerCase()).id(_id);

        XContentBuilder builder = jsonBuilder().startObject();
        for (Map.Entry entry : data.entrySet()){
            builder.field(String.valueOf(entry.getKey()), entry.getValue());
        }
        builder.endObject();
        updateRequest.doc(builder);

        return  client.update(updateRequest).get();
    }


    public Response multiMatchSearch(List<String> _indices, List<String> _types, String field, String value) throws EsOperationException {
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(value, "*" + field + "*");
        SearchResponse response = baseQuery(_indices, _types, queryBuilder);
        return getResponse(response);
    }

    public Response multiMatchSearch(List<String> _indices, String field, String value) throws EsOperationException {
        return multiMatchSearch(_indices, null, field, value);
    }

    public Response stringSearch(List<String> _indices, List<String> _types, String query_string) throws EsOperationException {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query_string);
        SearchResponse response = baseQuery(_indices, _types, queryBuilder);
        return getResponse(response);
    }

    public Response stringSearch(List<String> _indices, String query_string) throws EsOperationException {
        return stringSearch(_indices, null, query_string);
    }

    @Deprecated
    public Response dateRangeSearch(List<String> _indices, List<String> _types, Date start, Date end) {
        if (start == null) throw new EsOperationException("start date is null or empty...");
        end = end == null ? new Date() : end;

        long startTime = start.getTime();
        long endTime = end.getTime();

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("executionTime").gte(startTime).lte(endTime);

        SearchResponse response = baseQuery(_indices, _types, queryBuilder);

        return getResponse(response);
    }

    public Response dateRangeSearch(List<String> _indices, List<String> _types, String field, Date start, Date end) {
        if (start == null) throw new EsOperationException("start date is null or empty...");
        end = end == null ? new Date() : end;

        long startTime = start.getTime();
        long endTime = end.getTime();

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery(field).gte(startTime).lte(endTime);

        SearchResponse response = baseQuery(_indices, _types, queryBuilder);

        return getResponse(response);
    }

    @Deprecated
    public Response dateRangeSearch(List<String> _indices, Date start, Date end) {
        return null;
    }

    public Response dateRangeSearch(List<String> _indices, String field, Date start, Date end) {
        return dateRangeSearch(_indices, null, field, start, end);
    }

    @Deprecated
    public Response dateRangeForValueSearch(List<String> _indices, List<String> _types, String value, Date start, Date end) {
        if (start == null) throw new EsOperationException("start date is null or empty...");
        end = end == null ? new Date() : end;

        long startTime = start.getTime();
        long endTime = end.getTime();

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("executionTime").gte(startTime).lte(endTime);
        //QueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery(value, "rowData*").type(MultiMatchQueryBuilder.Type.PHRASE);
        QueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery(value, "rowData*").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).minimumShouldMatch("3");
        QueryBuilder mustQueryBuilder = QueryBuilders.boolQuery().must(rangeQueryBuilder).must(multiQueryBuilder);

        SearchResponse response = baseQuery(_indices, _types, mustQueryBuilder);

        return getResponse(response);
    }

    public Response dateRangeForValueSearch(List<String> _indices, List<String> _types, String field, String value, Date start, Date end) {
        if (start == null) throw new EsOperationException("start date is null or empty...");
        end = end == null ? new Date() : end;

        long startTime = start.getTime();
        long endTime = end.getTime();

        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field).gte(startTime).lte(endTime);
        //QueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery(value, "rowData*").type(MultiMatchQueryBuilder.Type.PHRASE);
        QueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery(value, "*").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).minimumShouldMatch("3");
        QueryBuilder mustQueryBuilder = QueryBuilders.boolQuery().must(rangeQueryBuilder).must(multiQueryBuilder);

        SearchResponse response = baseQuery(_indices, _types, mustQueryBuilder);

        return getResponse(response);
    }

    /**
     * 根据key-value字段查询数据
     * @param _index
     * @param _type
     * @param where
     * @return
     */
    public Response whereForSearch(String _index, String _type, Map<String, Object> where){
        if (_index == null) throw new EsOperationException("_index is null or empty...");
        if (_type == null) throw new EsOperationException("_type is null or empty...");
        if (where == null) throw new EsOperationException("where is null or empty...");

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        for(Map.Entry entry : where.entrySet()){
            QueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery(entry.getValue(), String.valueOf(entry.getKey())).type(MultiMatchQueryBuilder.Type.PHRASE);
            boolBuilder.must(multiQueryBuilder);
        }

        List<String> _indices = new ArrayList<>();
        _indices.add(_index);
        List<String> _types = new ArrayList<>();
        _types.add(_type);

        SearchResponse response = baseQuery(_indices, _types, boolBuilder);

        return getResponse(response);
    }

    @Deprecated
    public Response dateRangeForValueSearch(List<String> _indices, String value, Date start, Date end) {
        return null;
    }

    public Response dateRangeForValueSearch(List<String> _indices, String value, String field, Date start, Date end) {
        return dateRangeForValueSearch(_indices, null, field, value, start, end);
    }

    /**
     * 检查索引/类型是否存在
     * @param indexOrType
     * @return
     * @throws SqlParseException
     * @throws SQLFeatureNotSupportedException
     */
    public boolean exist(String indexOrType) throws SqlParseException, SQLFeatureNotSupportedException {
        if(StringUtils.isEmpty(indexOrType)) new EsOperationException("indexOrType can not be null...");
        if (searchDao == null) throw new EsOperationException("sql searchDao is null, You must call the queryAsSQL() method when this method is called...");

        String query = "show " + indexOrType;
        SqlElasticRequestBuilder requestBuilder =  searchDao.explain(query).explain();
        return !((GetIndexResponse) requestBuilder.get()).getMappings().isEmpty();
    }

    /**
     * 模拟sql查询
     * @param query
     * @return
     * @throws SqlParseException
     * @throws SQLFeatureNotSupportedException
     * @throws SQLFeatureNotSupportedException
     */
    public SqlResponse select(String query) throws SqlParseException, SQLFeatureNotSupportedException {
        if (searchDao == null) throw new EsOperationException("sql searchDao is null, You must call the queryAsSQL() method when this method is called...");
        SqlElasticSearchRequestBuilder select = (SqlElasticSearchRequestBuilder) searchDao.explain(query).explain();
        return new SqlResponse((SearchResponse)select.get());
    }

    public EsClient queryAsSQL(){
        if(this.searchDao == null) this.searchDao = new SearchDao(client);
        return this;
    }

    /**
     * sql翻译成es查询DSL
     * @param sql
     * @return
     * @throws SQLFeatureNotSupportedException
     * @throws SqlParseException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws IOException
     */
    public String explain(String sql) throws SQLFeatureNotSupportedException, SqlParseException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        if (searchDao == null) throw new EsOperationException("sql searchDao is null, you must call EsClient.queryAsSQL...");
        SqlElasticRequestBuilder requestBuilder = searchDao.explain(sql).explain();
        return requestBuilder.explain();
    }

    /**
     *
     * @param _index
     * @param _type
     * @return
     */
    /*public void delete(String _index, String _type) throws ExecutionException, InterruptedException {
        if(_type == null) throw new EsOperationException("_type is null...");

        DeleteByQueryRequestBuilder deleteQuery = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

        deleteQuery.setIndices(_index);
        if (_type != null) {
            deleteQuery.setTypes(_type);
        }
        deleteQuery.setQuery(QueryBuilders.matchAllQuery());

        deleteQuery.execute().actionGet();
    }*/

    /**
     * 删除数据
     * delete * from my_test/book
     * or delete * from my_test/book where id='1'
     * 此方法用到了delete-by-query插件,服务端也需要安装对应版本的插件才可以使用,否则会有各种异常
     * client对象要使用addPlugin(DeleteByQueryPlugin.class)使用插件
     * @param deleteStatement
     * @throws SQLFeatureNotSupportedException
     * @throws SqlParseException
     */
    public ActionResponse delete(String deleteStatement) throws SQLFeatureNotSupportedException, SqlParseException {
        if(StringUtils.isEmpty(deleteStatement)) throw new EsOperationException("deleteStatement can not be null..");
        if (searchDao == null) throw new EsOperationException("sql searchDao is null, you must call EsClient.queryAsSQL...");

        return searchDao.explain(deleteStatement).explain().get();
    }

    /**
     * 基础查询
     *
     * @param _indices
     * @param _types
     * @param queryBuilder
     * @return
     * @throws EsOperationException
     */
    private SearchResponse baseQuery(List<String> _indices, List<String> _types, QueryBuilder queryBuilder) throws EsOperationException {
        if (_indices == null || _indices.size() == 0) throw new EsOperationException("_indexes is null or empty...");

        SearchRequestBuilder requestBuilder = client.prepareSearch(_indices.toArray(new String[_indices.size()]));

        //requestBuilder.setSearchType(SearchType.QUERY_AND_FETCH);
        requestBuilder.setQuery(queryBuilder);
        if (_types != null && _types.size() > 0) {
            requestBuilder.setTypes(_types.toArray(new String[_types.size()]));
        }

        SearchResponse response = requestBuilder.setExplain(true).execute().actionGet();
        return response;
    }

    /**
     * @param ipAddress
     * @return
     */
    private byte[] getIpByte(String ipAddress) {
        List<String> ipV = Splitter.on(".").trimResults().omitEmptyStrings().splitToList(ipAddress);
        byte[] ipByte = new byte[ipV.size()];

        for (int i = 0; i < ipV.size(); i++) {
            ipByte[i] = (byte) (Integer.parseInt(ipV.get(i)) & 0xFF);//调整整数大小,byte的数值范围为-128~127
        }

        return ipByte;
    }

    /**
     * 查询返回结果转换
     *
     * @param indexResponse
     * @return
     * @throws EsOperationException
     */
    private Response translation(IndexResponse indexResponse) throws EsOperationException {
        Response response = null;
        if (indexResponse != null) {
            response = new Response();

            response.setId(indexResponse.getId());
            response.setIndex(indexResponse.getIndex());
            response.setType(indexResponse.getType());
            response.setVersion(indexResponse.getVersion());
            response.setCreated(indexResponse.isCreated());
        } else {
            throw new EsOperationException("indexResponse is null...");
        }
        return response;
    }

    /**
     * 获取查询结果
     *
     * @param searchResponse
     * @return
     */
    private List<Map<String, Object>> getSource(SearchResponse searchResponse) {
        if (searchResponse.getHits().getHits() == null) return null;
        List<Map<String, Object>> maps = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            maps.add(hit.getSource());
        }

        return maps;
    }

    /**
     * 转换成自己的Response
     *
     * @param searchResponse
     * @return
     */
    private Response getResponse(SearchResponse searchResponse) {
        if (searchResponse == null) return null;

        Response response = new Response();
        response.setSource(getSource(searchResponse));
        response.setTotal(searchResponse.getHits().getTotalHits());

        return response;
    }


    private Map<?, ?> objectToMap(Object obj) {
        if(obj == null)
            return null;

        return new org.apache.commons.beanutils.BeanMap(obj);
    }

    private static Map<String, Object> transBean2Map(Object obj) throws Exception{

        if(obj == null){
            return null;
        }
        Map<String, Object> map = new HashMap<>();

        BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor property : propertyDescriptors) {
            String key = property.getName();

            // 过滤class属性
            if (!key.equals("class")) {
                // 得到property对应的getter方法
                Method getter = property.getReadMethod();
                Object value = getter.invoke(obj);

                map.put(key, value);
            }

        }

        return map;

    }

    /**
     * destroy
     */
    public void destroy() {
        this.client.close();
        logger.info("destroy es client...");
    }

}
