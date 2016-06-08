package com.retail.datahub.es.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.es.model.Response <br/>
 * date: 2016/5/9 15:12 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class Response implements Serializable{

    private String index;

    private String type;

    private String id;

    private Long version;

    private Boolean created;

    private Long total;

    private List<Map<String, Object>> source;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Boolean isCreated() {
        return created;
    }

    public void setCreated(Boolean created) {
        this.created = created;
    }

    public List<Map<String, Object>>  getSource() {
        return source;
    }

    public void setSource(List<Map<String, Object>>  source) {
        this.source = source;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "Response{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", version=" + version +
                ", created=" + created +
                ", total=" + total +
                ", source='" + source + '\'' +
                '}';
    }
}
