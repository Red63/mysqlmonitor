package com.retail.datahub.model.common;

import com.retail.datahub.model.appmodel.MapEventBatchModel;
import com.retail.datahub.model.appmodel.MapRowData;
import com.retail.datahub.model.canalmodel.ColumnModel;
import com.retail.datahub.model.canalmodel.EventBatchModel;
import com.retail.datahub.model.canalmodel.RowDataModel;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述:<br/>TODO; <br/>
 * ClassName: com.retail.datahub.model.common.ModelConvert <br/>
 * date: 2016/5/9 17:51 <br/>
 *
 * @author Red(luohong@retail-tek.com)
 * @version 1.0.0
 */
public class ModelConvert {

    public static MapEventBatchModel convert(EventBatchModel source) {
        MapEventBatchModel targert = new MapEventBatchModel();

        if (source == null) return null;

        targert.setBatchId(source.getBatchId());
        targert.setDbName(source.getDbName());
        targert.setEventType(source.getEventType());
        targert.setEventDate(source.getEventDate());
        targert.setExecutionTime(source.getExecutionTime());
        targert.setLogicTableName(source.getLogicTableName());
        targert.setRealTableName(source.getRealTableName());


        MapRowData[] rowDatas = new MapRowData[source.getRowData().length];
        for (int i = 0; i < source.getRowData().length; i++) {
            rowDatas[i] = new MapRowData();

            rowDatas[i].setBeforeColumns(convertColumnToMap(source.getRowData()[i].getBeforeColumns()));
            rowDatas[i].setAfterColumns(convertColumnToMap(source.getRowData()[i].getAfterColumns()));
        }

        targert.setRowData(rowDatas);

        return targert;
    }

    public static Map<String, Object> convertColumnToMap(ColumnModel[] columnModels) {
        if (columnModels == null) return null;
        Map<String, Object> map = new HashMap<>();

        for (ColumnModel columnModel : columnModels) {

            if (("date".equals(columnModel.getType()) && "".equals(columnModel.getValue()))
                    || ("datetime".equals(columnModel.getType()) && "".equals(columnModel.getValue()))) {

                map.put(columnModel.getName(), null);
            } else {
                map.put(columnModel.getName(), columnModel.getValue());
            }
        }

        return map;
    }
}
