package com.retail.datahub.es.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retail.datahub.es.exception.EsOperationException;
import com.retail.datahub.es.util.JSONUtil;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.model.SqlResponse <br/>
 * date: 2016/6/6 14:30 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class SqlResponse {

    private SearchResponse searchResponse;

    private Set<Map<String, Object>> rows = new LinkedHashSet<>();

    private Map<String, Object> row = new HashMap<>();

    private boolean callChild = false;

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public SqlResponse(){}

    /**
     * 查询后的响应对象
     * @param searchResponse
     */
    public SqlResponse(SearchResponse searchResponse){
        this.searchResponse = searchResponse;
    }


    /**
     * 是否有聚合查询结果
     * @return
     */
    private boolean isAggregation(){
        if(searchResponse == null) return false;

        return searchResponse.getAggregations() != null;
    }

    /**
     * 是否有查询结果
     * @return
     */
    private boolean isHits(){
        if (searchResponse == null) return false;

        return searchResponse.getHits() != null;
    }


    /**
     * map对象转业务实体对象
     * @param map
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    /*private <T> T mapToObject(Map<String, Object> map, Class<T> clazz) throws Exception {
        if (map == null)
            return null;

        T obj = clazz.newInstance();

        org.apache.commons.beanutils.BeanUtils.populate(obj, map);

        return obj;
    }*/


    private <T> T mapToObject(Map<String, Object> map, Class<T> clazz) throws Exception{
        return JSONUtil.mapToBeanNotContainUnknown(map, clazz);
    }

    /**
     * 讲es的field域转换为你Object
     *
     * @param fields
     * @return
     */
    private Map<String, Object> toFieldsMap(Map<String, SearchHitField> fields) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, SearchHitField> entry : fields.entrySet()) {
            if (entry.getValue().values().size() > 1) {
                result.put(entry.getKey(), entry.getValue().values());
            } else {
                result.put(entry.getKey(), entry.getValue().value());
            }

        }
        return result;
    }

    /**
     * 获取hits结果
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> List<T> hitsResult(Class<T> clazz) throws Exception{
        if(isHits()){
            if (isAggregation()){
                throw new EsOperationException("The method of access errors, provides you with aggregationResult() method...");
            }
        }

        List<T> results = new ArrayList<>();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit searchHit : hits.getHits()) {
            if (searchHit.getSource() != null) {
                results.add(mapToObject(searchHit.getSource(), clazz));
            } else if (searchHit.getFields() != null) {
                Map<String, SearchHitField> fields = searchHit.getFields();
                results.add(mapToObject(toFieldsMap(fields), clazz));
            }

        }

        return results;
    }

    /**
     * 聚合查询返回结果
     * @return
     * @throws Exception
     */
    public Set<Map<String, Object>> aggregationResult() throws Exception {
        if(isHits()){
            if (!isAggregation()){
                throw new EsOperationException("The method of access errors, provides you with hitsResult() method...");
            }
        }

        String response = searchResponse.toString();

        Map<String, Object> responseMap = JSONObject.parseObject(response);
        Map<String, Object> aggregationMap = (Map<String, Object>) responseMap.get("aggregations");

        Map<String, Object> result = new HashMap<>();
        String parentKey = "";
        Map<String, Object> parentValue = null;

        for (Map.Entry entry : aggregationMap.entrySet()){
            parentKey = String.valueOf(entry.getKey());
            parentValue = (Map<String, Object>) entry.getValue();
        }
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) parentValue.get("buckets");

        if(buckets == null){
            if(parentValue.get("value") != null){
                row.put(parentKey, parentValue.get("value"));
            } else {
                row.put(parentKey, null);
            }

            rows.add(row);
        } else {
            getSubBuckets(buckets, parentKey);
        }

        return rows;
    }

    /**
     * 递归子节点
     * @param buckets
     * @param prentKey
     * @return
     */
    private Set<Map<String, Object>> getSubBuckets(List<Map<String, Object>> buckets, String prentKey){

        for (Map<String, Object> map : buckets){

            if(!callChild){
                row = new HashMap<>();
            }

            row.put(prentKey, map.get("key"));
            String subKey = subKey(map);

            List<Map<String, Object>> subs = null;
            if(StringUtils.isNotEmpty(subKey)){
                subs = (List<Map<String, Object>>) ((Map<String, Object>) map.get(subKey)).get("buckets");
            }

            if(subs == null){
                Map<String, Object> endMap = (Map<String, Object>) map.get(subKey);
                if(endMap != null){
                    if(endMap.get("value") != null){
                        row.put(subKey, endMap.get("value"));
                    } else {
                        row.put(subKey, null);
                    }
                }

                callChild = false;
            } else {
                callChild = true;
                getSubBuckets(subs, subKey);
            }

            rows.add(row);
        }
        return rows;
    }

    /**
     * 获取子节点的key名字
     * @param bucket
     * @return
     */
    private String subKey(Map<String, Object> bucket){
        String subkey = "";

        for (Map.Entry entry : bucket.entrySet()){
            if (!StringUtils.equals("doc_count", String.valueOf(entry.getKey()))
                    && !StringUtils.equals("doc_count_error_upper_bound", String.valueOf(entry.getKey()))
                    && !StringUtils.equals("key", String.valueOf(entry.getKey()))){

                subkey = String.valueOf(entry.getKey());
            }
        }

        return subkey;
    }

}
