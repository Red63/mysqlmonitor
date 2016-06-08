package com.retail.datahub.test.domian;

import java.io.Serializable;
import java.util.Date;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.test.domian.User <br/>
 * date: 2016/6/2 10:41 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class User implements Serializable{

    private Long id;

    private String name;

    private String code;

    private Integer age;

    private Date modifyCreate;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Date getModifyCreate() {
        return modifyCreate;
    }

    public void setModifyCreate(Date modifyCreate) {
        this.modifyCreate = modifyCreate;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", code='" + code + '\'' +
                ", age=" + age +
                ", modifyCreate=" + modifyCreate +
                '}';
    }
}
