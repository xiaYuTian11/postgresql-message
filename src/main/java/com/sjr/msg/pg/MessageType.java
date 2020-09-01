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
    BEGIN(4),
    /**
     * 表关系
     **/
    RELATION(5),
    /***
     * 提交
     * **/
    COMMIT(6),
    /**
     * 插入
     **/
    INSERT(1),
    /**
     * 更新
     **/
    UPDATE(2),
    /**
     * 删除
     **/
    DELETE(3),

    TYPE(7),

    ORIGIN(8),

    TRUNCATE(9);

    MessageType(int code) {
        this.code = code;
    }

    private int code;

    public int getCode() {
        return code;
    }

    public MessageType setCode(int code) {
        this.code = code;
        return this;
    }

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
