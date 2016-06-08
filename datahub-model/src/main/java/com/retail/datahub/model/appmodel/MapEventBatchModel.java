package com.retail.datahub.model.appmodel;

import java.io.Serializable;

/**
 * Created by red on 2016/3/11.
 */
public class MapEventBatchModel implements Serializable{
    private Long batchId;

    private String dbName;

    private String logicTableName;

    private String realTableName;

    /**
     * Note: maybe not the very exact execution time, we may manually merge some continuous events in batch,
     * will use the time of the first one
     */
    private long executionTime;

    private String eventType;

    private MapRowData[] rowData;

    private String eventDate;

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public String getRealTableName() {
        return realTableName;
    }

    public void setRealTableName(String realTableName) {
        this.realTableName = realTableName;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public MapRowData[] getRowData() {
        return rowData;
    }

    public void setRowData(MapRowData[] rowData) {
        this.rowData = rowData;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapEventBatchModel that = (MapEventBatchModel) o;

        if (!dbName.equals(that.dbName)) return false;
        if (!realTableName.equals(that.realTableName)) return false;
        if (!eventDate.equals(that.eventDate)) return false;
        return eventType.equals(that.eventType);

    }

    @Override
    public int hashCode() {
        int result = dbName.hashCode();
        result = 31 * result + realTableName.hashCode();
        result = 31 * result + eventType.hashCode();
        result = 31 * result + eventDate.hashCode();
        return result;
    }

}
