package com.sjr.msg.message;

import lombok.ToString;

import java.util.*;

/***
 * 消息封装
 * @author doge
 * **/
@ToString
public class PgOutMessage {

    /***
     * 操作类型
     * */
    private MessageType opt;
    /**
     * 表名
     **/
    private String tableName;
    /***
     * 主键值
     * **/
    private String pkValue;
    /***
     * 主键名称
     * */
    private String pkName;
    /***
     * 消息编号
     * **/
    private long lsnNum;
    /**
     * 旧数据
     **/
    private Map<String, String> oldData;
    /***
     * 老数据
     * **/
    private Map<String, String> newData;

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

    public PgOutMessage addOldData(String key, String value) {
        if (oldData == null) {
            oldData = new HashMap<>(10);
        }
        oldData.put(key, value);
        return this;
    }

    public PgOutMessage addNewData(String key, String value) {
        if (newData == null) {
            newData = new HashMap<>(10);
        }
        newData.put(key, value);
        return this;
    }

    public Set<String> getChangeKey() {
        if (oldData == null && newData == null) {
            return new HashSet<>(0);
        }
        if (oldData == null) {
            return newData.keySet();
        }
        if (newData == null) {
            return oldData.keySet();
        }
        Set<String> changeSet = new HashSet<>(10);
        newData.forEach((key, value) -> {
            String oldValue = oldData.get(key);
            if (!Objects.equals(oldValue, value)) {
                changeSet.add(key);
            }
        });
        return changeSet;
    }

    public boolean isChange(String[] fields) {
        if (oldData == null || newData == null) {
            return true;
        }
        Map<String, String> map1 = new HashMap<>(newData.size() + 2);
        Map<String, String> map2 = new HashMap<>(oldData.size() + 2);
        Arrays.stream(fields).forEach(field -> {
            map1.put(field, newData.get(field));
            map2.put(field, oldData.get(field));
        });
        return CollectionUtil.compareMapNe(map1, map2);
    }

}
