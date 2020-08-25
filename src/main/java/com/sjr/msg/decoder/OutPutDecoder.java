package com.sjr.msg.decoder;


import com.sjr.msg.message.MessageType;
import com.sjr.msg.message.PgOutMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;

/***
 * 输出解码器
 * @author doge
 * **/
public class OutPutDecoder {

    private static final OutPutDecoder DECODER = new OutPutDecoder();

    private static final Logger LOGGER = LoggerFactory.getLogger(OutPutDecoder.class);

    private HashMap<Integer, Table> tableCache = new HashMap<>(10);
    private HashMap<String, String> tablePkCache = new HashMap<>(10);

    private long currentLsn;

    private OutPutDecoder() {
    }

    public static OutPutDecoder getInstance() {
        return DECODER;
    }

    public PgOutMessage process(ByteBuffer buffer) {
        MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
            case BEGIN:
                return handBegin(buffer);
            case RELATION:
                return handRelation(buffer);
            case INSERT:
                return handInsert(buffer);
            case UPDATE:
                return handUpdate(buffer);
            case DELETE:
                return handDelete(buffer);
            case COMMIT:
                return handCommit(buffer);
            default:
                return null;
        }
    }

    /***
     * 处理提交信息
     * **/
    private PgOutMessage handCommit(ByteBuffer buffer) {
        // flags, currently unused
        int flags = buffer.get();
        // LSN of the commit
        long lsn = buffer.getLong();
        // End LSN of the transaction
        long endLsn = buffer.getLong();
        LOGGER.info("message: {}", MessageType.BEGIN);
        LOGGER.info("Flags: {} (currently unused and most likely 0)", flags);
        LOGGER.info("Commit LSN: {}", lsn);
        LOGGER.info("End LSN of transaction: {}", endLsn);
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setOpt(MessageType.COMMIT);
        pgOutMessage.setLsnNum(currentLsn);
        return pgOutMessage;
    }

    /**
     * 处理事务开始信息
     **/
    private PgOutMessage handBegin(ByteBuffer buffer) {
        long lsn = buffer.getLong();
        LOGGER.info("message: {}", MessageType.BEGIN);
        LOGGER.info("Final LSN of transaction: {}", lsn);
        this.currentLsn = lsn;
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setLsnNum(lsn);
        pgOutMessage.setOpt(MessageType.BEGIN);
        return pgOutMessage;
    }

    /***
     * 处理表relation
     * **/
    private PgOutMessage handRelation(ByteBuffer buffer) {
        //表id
        int relationId = buffer.getInt();
        //表空间名称
        String schemaName = DecodeUtils.readString(buffer);
        //表名
        String tableName = DecodeUtils.readString(buffer);
        if (!tablePkCache.containsKey(tableName)) {
            LOGGER.warn("无对应表:{} 相关信息", tableName);
            return null;
        }
        int replicaIdentityId = buffer.get();
        //表字段数
        short columnCount = buffer.getShort();
        LOGGER.info("relationId:{} schemaName:{} tableName:{} replicaIdentityId:{} columnCount:{}", relationId, schemaName, tableName, replicaIdentityId, columnCount);
        Table table = new Table(relationId, schemaName, tableName, replicaIdentityId, columnCount);
        for (int i = 0; i < columnCount; i++) {
            byte flags = buffer.get();
            String columnName = DecodeUtils.unquoteIdentifierPart(DecodeUtils.readString(buffer));
            //TODO 类型暂时不需要,不做处理
            int columnType = buffer.getInt();
            int attypmod = buffer.getInt();
            table.addColumnName(columnName);
        }

        tableCache.put(relationId, table);
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setTableName(tableName);
        pgOutMessage.setOpt(MessageType.RELATION);
        pgOutMessage.setLsnNum(currentLsn);
        pgOutMessage.setPkName(tablePkCache.get(tableName));

        return pgOutMessage;
    }

    /***
     * 处理插入消息
     * **/
    private PgOutMessage handInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            LOGGER.warn("无对应的表信息,relationId:{}", relationId);
            return null;
        }
        final HashMap<String, String> map = DecodeUtils.resolveColumnsFromStreamTupleData(buffer, table);
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setPkName(table.getPkName());
        pgOutMessage.setPkValue(map.get(table.getPkName()));
        pgOutMessage.setLsnNum(currentLsn);
        pgOutMessage.setTableName(table.getTableName());
        pgOutMessage.setNewData(map);
        pgOutMessage.setOpt(MessageType.INSERT);
        return pgOutMessage;
    }

    /***
     *
     * 处理更新消息
     * **/
    private PgOutMessage handUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            LOGGER.warn("无对应的表信息,relationId:{}", relationId);
            return null;
        }
        HashMap<String, String> oldData = null;
        if ('O' == tupleType || 'K' == tupleType) {
            oldData = DecodeUtils.resolveColumnsFromStreamTupleData(buffer, table);
            tupleType = (char) buffer.get();
        }
        final HashMap<String, String> newData = DecodeUtils.resolveColumnsFromStreamTupleData(buffer, table);

        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setLsnNum(currentLsn);
        pgOutMessage.setOpt(MessageType.UPDATE);
        pgOutMessage.setPkName(table.getPkName());
        pgOutMessage.setPkValue(newData.get(table.getPkName()));
        pgOutMessage.setTableName(table.getTableName());
        pgOutMessage.setOldData(oldData);
        pgOutMessage.setNewData(newData);
        return pgOutMessage;
    }

    /***
     * 处理删除消息
     * **/
    private PgOutMessage handDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            LOGGER.warn("无对应的表信息,relationId:{}", relationId);
            return null;
        }
        final HashMap<String, String> oldData = DecodeUtils.resolveColumnsFromStreamTupleData(buffer, table);
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setTableName(table.getTableName());
        pgOutMessage.setLsnNum(currentLsn);
        pgOutMessage.setPkName(table.getPkName());
        pgOutMessage.setPkValue(oldData.get(table.getPkName()));
        pgOutMessage.setOpt(MessageType.DELETE);
        pgOutMessage.setOldData(oldData);
        return pgOutMessage;
    }

    public void addPkCache(String key, String value) {
        tablePkCache.put(key, value);
    }

}
