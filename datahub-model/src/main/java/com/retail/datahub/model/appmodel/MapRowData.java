package com.retail.datahub.model.appmodel;

import java.util.Map;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.model.appmodel.MapRowData <br/>
 * date: 2016/5/9 17:54 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class MapRowData {

    private Map<String, Object> beforeColumns;

    private Map<String, Object> afterColumns;

    public Map getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(Map<String, Object> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public Map getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(Map<String, Object> afterColumns) {
        this.afterColumns = afterColumns;
    }
}
