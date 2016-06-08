package com.retail.datahub.model.canalmodel;

import com.retail.datahub.model.canalmodel.ColumnModel;

import java.io.Serializable;

/**
 * Created by red on 2016/3/11.
 */
public class RowDataModel implements Serializable{

    private ColumnModel[] beforeColumns;

    private ColumnModel[] afterColumns;

    public ColumnModel[] getBeforeColumns() {

        return beforeColumns;
    }

    public void setBeforeColumns(ColumnModel[] beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public ColumnModel[] getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(ColumnModel[] afterColumns) {
        this.afterColumns = afterColumns;
    }
}
