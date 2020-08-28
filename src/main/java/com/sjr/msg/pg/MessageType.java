package com.sjr.msg.pg;

/**
 * 消息操作类型枚举
 *
 * @author TMW
 */
public enum MessageType {
    /**
     * 事务开始
     **/
    BEGIN,
    /**
     * 表关系
     **/
    RELATION,
    /***
     * 提交
     * **/
    COMMIT,
    /**
     * 插入
     **/
    INSERT,
    /**
     * 更新
     **/
    UPDATE,
    /**
     * 删除
     **/
    DELETE,

    TYPE,

    ORIGIN,

    TRUNCATE;

    public static MessageType forType(char type) {
        switch (type) {
            case 'R':
                return RELATION;
            case 'B':
                return BEGIN;
            case 'C':
                return COMMIT;
            case 'I':
                return INSERT;
            case 'U':
                return UPDATE;
            case 'D':
                return DELETE;
            case 'Y':
                return TYPE;
            case 'O':
                return ORIGIN;
            case 'T':
                return TRUNCATE;
            default:
                throw new IllegalArgumentException("Unsupported message type: " + type);
        }
    }

}
