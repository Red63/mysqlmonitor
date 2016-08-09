package com.retail.datahub.test;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.JsonTest <br/>
 * date: 2016/5/10 16:00 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class JsonTest {

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();

        map.put("id", "1234");
        map.put("name", "xsd");
        map.put("other", null);

        System.out.println(map.get("adcdef"));

        User user = new User();
        user.setId("1234");
        user.setName("xsd");
        //user.setOther(null);

        User user1 = new User();
        user1.setId("1234");
        user1.setName("xsd");

        System.out.println("user:" + user);
        System.out.println("user1:" + user1);


        System.out.println("json:" + JSONObject.toJSONString(user, SerializerFeature.WriteMapNullValue));

        System.out.println(6 % 2);


    }

    static class User {
        private String id;

        private String name;

        private Integer other;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getOther() {
            return other;
        }

        public void setOther(Integer other) {
            this.other = other;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + id.hashCode();
            result = 31 * result + name.hashCode();
            result = 31 * result + (other == null ? 0 : other.intValue());

            return result;
        }

    }

    @Test
    public void test2json(){
        String json1="[{\"afterColumns\":[{\"key\":false,\"name\":\"heartbeat_count\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":true,\"value\":\"32\"},{\"key\":false,\"name\":\"heartbeat_time\",\"null\":false,\"type\":\"datetime\",\"updated\":true,\"value\":\"2016-07-25 12:35:36\"},{\"key\":false,\"name\":\"heartbeat_id\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"1\"}],\"beforeColumns\":[{\"key\":false,\"name\":\"heartbeat_count\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"31\"},{\"key\":false,\"name\":\"heartbeat_time\",\"null\":false,\"type\":\"datetime\",\"updated\":false,\"value\":\"2016-07-25 12:25:36\"},{\"key\":false,\"name\":\"heartbeat_id\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"1\"}]}]";

        String json2="[{\"afterColumns\":[{\"key\":false,\"name\":\"heartbeat_count\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":true,\"value\":\"32\"},{\"key\":false,\"name\":\"heartbeat_time\",\"null\":false,\"type\":\"datetime\",\"updated\":true,\"value\":\"2016-07-25 12:35:36\"},{\"key\":false,\"name\":\"heartbeat_id\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"1\"}],\"beforeColumns\":[{\"key\":false,\"name\":\"heartbeat_count\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"31\"},{\"key\":false,\"name\":\"heartbeat_time\",\"null\":false,\"type\":\"datetime\",\"updated\":false,\"value\":\"2016-07-25 12:25:36\"},{\"key\":false,\"name\":\"heartbeat_id\",\"null\":false,\"type\":\"bigint(20)\",\"updated\":false,\"value\":\"1\"}]}]";

        System.out.println(!StringUtils.equals(json1, json2));
    }

    private static String upJson="";

    @Test
    public void testStatic(){
        upJson = "123";

        System.out.println(StringUtils.equals("", upJson));
    }
}
