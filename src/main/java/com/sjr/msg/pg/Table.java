package com.sjr.msg.pg;

import java.util.ArrayList;
import java.util.List;

public class Table {

    private int relationId;

    private String schemaName;

    private String tableName;

    private int replicaIdentityId;

    private short columnCount;

    private String pkName;

    private List<String> columnNames;

    public Table(int relationId, String schemaName, String tableName,String pkName, int replicaIdentityId, short columnCount) {
        this.relationId = relationId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.pkName = pkName;
        this.replicaIdentityId = replicaIdentityId;
        this.columnCount = columnCount;
    }

    public Table() {
    }

    public int getRelationId() {
        return relationId;
    }

    public Table setRelationId(int relationId) {
        this.relationId = relationId;
        return this;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Table setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public Table setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public int getReplicaIdentityId() {
        return replicaIdentityId;
    }

    public Table setReplicaIdentityId(int replicaIdentityId) {
        this.replicaIdentityId = replicaIdentityId;
        return this;
    }

    public short getColumnCount() {
        return columnCount;
    }

    public Table setColumnCount(short columnCount) {
        this.columnCount = columnCount;
        return this;
    }

    public String getPkName() {
        return pkName;
    }

    public Table setPkName(String pkName) {
        this.pkName = pkName;
        return this;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Table setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    public void addColumnName(String columnName){
        if(columnNames == null){
            columnNames = new ArrayList<>();
        }
        columnNames.add(columnName);
    }
}
