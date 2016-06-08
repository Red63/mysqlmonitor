package com.retail.datahub.es.util;

import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
/**
 * 
 * json工具类
 * 
 * @author lc
 * @data 2015年10月29日 下午5:13:57
 * 
 */
public class JSONUtil {
    private final static ObjectMapper objectMapper = new ObjectMapper();
    /**
     * 创建一个新的实例 JSONUtil.
     *
     */
    public JSONUtil() {
    }
    public static ObjectMapper getInstance() {
        return objectMapper;
    }
    /**
     * 实体转json
     * 
     * @param obj
     *            转换的实体
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:15:55
     */
    public static String beanToJson(Object obj) throws Exception {
        return objectMapper.writeValueAsString(obj);
    }
    /**
     * 实体转byte数组
     * 
     * @param obj
     *            转换的实体
     * @return
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:16:16
     */
    public static byte[] beanTobytes(Object obj) {
        String json = null;
        try {
            json = objectMapper.writeValueAsString(obj);
            return json.getBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * json转实体
     * 
     * @param jsonStr
     *            要转换的json数据
     * @param clazz
     *            实体类
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:17:00
     */
    public static <T> T jsonToBean(String jsonStr, Class<T> clazz) throws Exception {
        return objectMapper.readValue(jsonStr, clazz);
    }
    /**
     * 移除不需要的字段属性
     * 
     * @param jsonStr
     * @param clazz
     * @return
     * @throws Exception
     * @date 2016年1月5日 下午2:58:55
     * @author lc
     */
    public static <T> T jsonToBeanNotContainUnknown(String jsonStr, Class<T> clazz) throws Exception {
        objectMapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(jsonStr, clazz);
    }
    /**
     * json转map
     * 
     * @param jsonStr
     *            要转换的json数据
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:17:11
     */
    @SuppressWarnings("unchecked")
    public static <T> Map<String, Object> jsonToMap(String jsonStr) throws Exception {
        return objectMapper.readValue(jsonStr, Map.class);
    }
    /**
     * json转map
     * 
     * @param jsonStr
     *            要转换的json数据
     * @param clazz
     *            实体类
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:17:39
     */
    public static <T> Map<String, T> jsonToMap(String jsonStr, Class<T> clazz) throws Exception {
        Map<String, Map<String, Object>> map = objectMapper.readValue(jsonStr, new TypeReference<Map<String, T>>() {
        });
        Map<String, T> result = new HashMap<String, T>();
        for (Entry<String, Map<String, Object>> entry : map.entrySet()) {
            result.put(entry.getKey(), mapToBean(entry.getValue(), clazz));
        }
        return result;
    }
    /**
     * bytes 数据转换为对象
     * 
     * @param bytes
     *            字节数据
     * @param className
     *            对象类名
     * @return 当 className为list时返回 list<LinkedHashMap>
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午3:56:29
     */
    public static <T> Object bytesToBean(byte[] bytes, String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        String json = new String(bytes);
        if (List.class.isAssignableFrom(clazz)) {
            return jsonToList(json);
        } else if (Map.class.isAssignableFrom(clazz)) {
            return jsonToMap(json);
        } else {
            try {
                return JSONUtil.jsonToBean(json, clazz);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    /**
     * byte数组转实体
     * 
     * @param bytes
     *            要转换的json数据
     * @param clazz
     *            实体类
     * @return
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:18:04
     */
    public static <T> T bytesToBean(byte[] bytes, Class<T> clazz) {
        String json = new String(bytes);
        try {
            return JSONUtil.jsonToBean(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static <T> T bytesToBeanNotContainUnknown(byte[] bytes, Class<T> clazz) {
        objectMapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String json = new String(bytes);
        try {
            return JSONUtil.jsonToBean(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static <T> T mapToBeanNotContainUnknown(Map map, Class<T> clazz) {
        objectMapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.convertValue(map, clazz);
    }
    /**
     * byte数组转列表
     * 
     * @param bytes
     *            byte数组
     * @param clazz
     *            实体类
     * @return
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:18:29
     */
    public static <T> List<T> bytesToList(byte[] bytes, Class<T> clazz) {
        String json = new String(bytes);
        try {
            return JSONUtil.jsonToList(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * byte数组转map
     * 
     * @param bytes
     *            byte数组
     * @param clazz
     *            实体类
     * @return
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:19:05
     */
    public static <T> Map<String, T> bytesToMap(byte[] bytes, Class<T> clazz) {
        String json = new String(bytes);
        try {
            return JSONUtil.jsonToMap(json, clazz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 实体转map
     * 
     * @param obj
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:54:06
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> beanToMap(Object obj) throws Exception {
        return JSONUtil.jsonToBean(JSONUtil.beanToJson(obj), Map.class);
    }
    /**
     * 实体转map Map<String, Object>
     * 
     * @param obj
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月12日 下午5:21:46
     */
    public static Map<String, Object> beanToMapObject(Object obj) throws Exception {
        return JSONUtil.jsonToMap(JSONUtil.beanToJson(obj));
    }
    /**
     * 实体转linkedHashMap
     * 
     * @param obj
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:54:23
     */
    @SuppressWarnings("unchecked")
    public static LinkedHashMap<String, String> beanToLinkedHashMap(Object obj) throws Exception {
        return JSONUtil.jsonToBean(JSONUtil.beanToJson(obj), LinkedHashMap.class);
    }
    /**
     * 
     * json转列表
     * 
     * @param jsonArrayStr
     *            json数据
     * @param clazz
     *            实体类
     * @return
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:19:27
     */
    public static <T> List<T> jsonToList(String jsonArrayStr, Class<T> clazz) throws Exception {
        if (clazz.isEnum()) {
            List<String> list = objectMapper.readValue(jsonArrayStr, new TypeReference<List<String>>() {
            });
            List result = new ArrayList();
            for (String i : list) {
                T[] ts = clazz.getEnumConstants();
                for (T t : ts) {
                    Enum<?> tempEnum = (Enum<?>) t;
                    if (i.equals(tempEnum.name())) {
                        result.add(tempEnum);
                        continue;
                    }
                }
            }
            return result;
        } else {
            List<Map<String, Object>> list = objectMapper.readValue(jsonArrayStr, new TypeReference<List<T>>() {
            });
            List<T> result = new ArrayList<T>();
            for (Map<String, Object> map : list) {
                result.add(mapToBean(map, clazz));
            }
            return result;
        }
    }
    /**
     * json转list
     * 
     * @param jsonArrayStr
     *            json数据
     * @return 返回结果为 list<linkedHashMap>
     * @throws Exception
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午4:23:17
     */
    public static List<Object> jsonToList(String jsonArrayStr) throws Exception {
        List<Object> list = objectMapper.readValue(jsonArrayStr, new TypeReference<List<Object>>() {
        });
        return list;
    }
    /**
     * map转实体
     * 
     * @param map
     *            map对象
     * @param clazz
     *            实体类
     * @return
     * @exception
     * @auth lc
     * @date 2015年11月2日 下午1:22:30
     */
    public static <T> T mapToBean(Map map, Class<T> clazz) {
        return objectMapper.convertValue(map, clazz);
    }

    /*
     * @SuppressWarnings("unchecked") public static void main(String[] args) { List<TestBean> beans = new ArrayList<TestBean>(); Map<String ,TestBean> maps=new HashMap<String, TestBean>();
     * 
     * for (int i = 0; i < 2; i++) { TestBean bean = new TestBean(); bean.setDate(new Date()); bean.setName(i + ""); beans.add(bean); maps.put(i+"", bean); } System.out.println(maps.getClass());
     * byte[] bytes=JSONUtil.beanTobytes(maps); try { HashMap<String ,LinkedHashMap<String, Object>> obj=(HashMap<String ,LinkedHashMap<String, Object>>) JSONUtil.bytesToBean(bytes,
     * "java.util.HashMap"); Iterator<String> iterator= obj.keySet().iterator(); while (iterator.hasNext()) { String key=(String) iterator.next(); System.out.println(JSONUtil.mapToBean(obj.get(key),
     * TestBean.class)); } } catch (Exception e) { e.printStackTrace(); } bytes = JSONUtil.beanTobytes(beans); List<LinkedHashMap<String, Object>> ss = null; try { ss = (List<LinkedHashMap<String,
     * Object>>) JSONUtil.bytesToBean( bytes, "java.util.ArrayList"); } catch (Exception e) { e.printStackTrace(); } for (LinkedHashMap<String, Object> map : ss) { try {
     * System.out.println(JSONUtil.mapToBean(map, TestBean.class) .toString()); } catch (Exception e) { e.printStackTrace(); } } }
     */
}