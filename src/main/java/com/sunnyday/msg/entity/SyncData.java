package com.sunnyday.msg.entity;

import lombok.ToString;

import java.util.Map;

/**
 * 同步监狱局数据模型
 *
 * @author TMW
 * @date 2020/9/1 9:47
 */
@ToString
public class SyncData {
    /**
     * 操作类型，1--insert，2--update，3--delete
     */
    private int optCode;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 主键值
     */
    private String pkValue;
    /**
     * 主键名称
     */
    private String pkName;

    /**
     * 只涉及到变化列的旧数据
     */
    private Map<String, String> oldData;
    /**
     * 只涉及到变化列的老数据
     */
    private Map<String, String> newData;

    public int getOptCode() {
        return optCode;
    }

    public SyncData setOptCode(int optCode) {
        this.optCode = optCode;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public SyncData setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getPkValue() {
        return pkValue;
    }

    public SyncData setPkValue(String pkValue) {
        this.pkValue = pkValue;
        return this;
    }

    public String getPkName() {
        return pkName;
    }

    public SyncData setPkName(String pkName) {
        this.pkName = pkName;
        return this;
    }

    public Map<String, String> getOldData() {
        return oldData;
    }

    public SyncData setOldData(Map<String, String> oldData) {
        this.oldData = oldData;
        return this;
    }

    public Map<String, String> getNewData() {
        return newData;
    }

    public SyncData setNewData(Map<String, String> newData) {
        this.newData = newData;
        return this;
    }
}
