package com.retail.datahub.test;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;

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

    public static void main(String[] args){
        Map<String, Object> map = new HashMap<>();

        map.put("id","1234");
        map.put("name", "xsd");
        map.put("other", null);

        User user = new User();
        user.setId("1234");
        user.setName("xsd");
        //user.setOther(null);



        System.out.println("json:" + JSONObject.toJSONString(user, SerializerFeature.WriteMapNullValue));

        System.out.println(6%2);
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
    }
}
