package com.sjr.msg.pg;

import lombok.ToString;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 消息封装
 *
 * @author TMW
 */
@ToString
public class PgOutMessage {

    /**
     * 操作类型
     */
    private MessageType opt;
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
     * 消息编号
     */
    private long lsnNum;
    /**
     * 旧数据
     */
    private Map<String, String> oldData;
    /**
     * 老数据
     */
    private Map<String, String> newData;
    /**
     * 变化的列
     */
    private Set<String> changeKey;

    public MessageType getOpt() {
        return opt;
    }

    public PgOutMessage setOpt(MessageType opt) {
        this.opt = opt;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public PgOutMessage setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getPkValue() {
        return pkValue;
    }

    public PgOutMessage setPkValue(String pkValue) {
        this.pkValue = pkValue;
        return this;
    }

    public long getLsnNum() {
        return lsnNum;
    }

    public PgOutMessage setLsnNum(long lsnNum) {
        this.lsnNum = lsnNum;
        return this;
    }

    public Map<String, String> getOldData() {
        return oldData;
    }

    public PgOutMessage setOldData(Map<String, String> oldData) {
        this.oldData = oldData;
        return this;
    }

    public String getPkName() {
        return pkName;
    }

    public PgOutMessage setPkName(String pkName) {
        this.pkName = pkName;
        return this;
    }

    public Map<String, String> getNewData() {
        return newData;
    }

    public PgOutMessage setNewData(Map<String, String> newData) {
        this.newData = newData;
        return this;
    }

    public PgOutMessage setChangeKey(Set<String> changeKey) {
        this.changeKey = changeKey;
        return this;
    }

    @SuppressWarnings("all")
    public Set<String> getChangeKey() {
        if (changeKey == null) {
            if (oldData == null && newData == null) {
                changeKey = new HashSet<>(0);
            }
            if (oldData == null && newData != null) {
                changeKey = newData.keySet();
            }
            if (newData == null && oldData != null) {
                changeKey = oldData.keySet();
            }
            changeKey = new HashSet<>(10);
            newData.forEach((key, value) -> {
                String oldValue = oldData.get(key);
                if (!Objects.equals(oldValue, value)) {
                    changeKey.add(key);
                }
            });
        }

        return changeKey;
    }

}
