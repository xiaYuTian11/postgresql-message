package com.sjr.msg.pg;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * pgoutput 解析插件
 *
 * @author TMW
 * @date 2020/8/28 17:36
 */
@Slf4j
public class OutPutPlugin {
    private static OutPutPlugin OUT_PUT_PLUGIN;

    /**
     * 表的缓存
     */
    private final Map<Integer, Table> tableCache = new ConcurrentHashMap<>(16);
    /**
     * 表对应的主键缓存
     */
    private final Map<String, String> tablePkCache = new ConcurrentHashMap<>(16);

    private long currentLsn;

    public static synchronized OutPutPlugin getInstance() {
        if (OUT_PUT_PLUGIN == null) {
            OUT_PUT_PLUGIN = new OutPutPlugin();
        }
        return OUT_PUT_PLUGIN;
    }

    /**
     * 解析消息
     *
     * @param buffer
     * @return
     */
    public PgOutMessage process(ByteBuffer buffer) {
        MessageType messageType = MessageType.forType((char) buffer.get());
        switch (messageType) {
            case BEGIN:
                return handBegin(buffer);
            case COMMIT:
                return handCommit(buffer);
            case RELATION:
                return handRelation(buffer);
            case INSERT:
                return handInsert(buffer);
            case UPDATE:
                return handUpdate(buffer);
            case DELETE:
                return handDelete(buffer);
            default:
                log.warn("Message Type {} skipped, not processed.", messageType);
                return null;
        }
    }

    /**
     * 处理事务开始信息
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handBegin(ByteBuffer buffer) {
        long lsn = buffer.getLong();
        log.info("message: {}", MessageType.BEGIN);
        log.info("Final LSN of transaction: {}", lsn);
        this.currentLsn = lsn;
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setLsnNum(lsn);
        pgOutMessage.setOpt(MessageType.BEGIN);
        return pgOutMessage;
    }

    /**
     * 处理提交信息
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handCommit(ByteBuffer buffer) {
        // flags, currently unused
        int flags = buffer.get();
        // LSN of the commit
        long lsn = buffer.getLong();
        // End LSN of the transaction
        long endLsn = buffer.getLong();
        log.info("message: {}", MessageType.BEGIN);
        log.info("Flags: {} (currently unused and most likely 0)", flags);
        log.info("Commit LSN: {}", lsn);
        log.info("End LSN of transaction: {}", endLsn);
        PgOutMessage pgOutMessage = new PgOutMessage();
        pgOutMessage.setOpt(MessageType.COMMIT);
        pgOutMessage.setLsnNum(currentLsn);
        return pgOutMessage;
    }

    /**
     * 处理表relation
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handRelation(ByteBuffer buffer) {
        //表id
        int relationId = buffer.getInt();
        //表空间名称
        String schemaName = DecodeUtils.readString(buffer);
        //表名
        String tableName = DecodeUtils.readString(buffer);
        if (!tablePkCache.containsKey(tableName)) {
            log.warn("无对应表:{} 相关信息", tableName);
            return null;
        }
        int replicaIdentityId = buffer.get();
        //表字段数
        short columnCount = buffer.getShort();
        log.info("relationId:{} schemaName:{} tableName:{} replicaIdentityId:{} columnCount:{}", relationId, schemaName, tableName, replicaIdentityId, columnCount);
        Table table = new Table(relationId, schemaName, tableName, tablePkCache.get(tableName), replicaIdentityId, columnCount);
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

    /**
     * 处理插入消息
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        // Always 'N" for inserts
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            log.warn("无对应的表信息,relationId:{}", relationId);
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

    /**
     * 处理更新消息
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            log.warn("无对应的表信息,relationId:{}", relationId);
            return null;
        }
        HashMap<String, String> oldData = null;
        // When reading the tuple-type, we could get 3 different values, 'O', 'K', or 'N'.
        // 'O' (Optional) - States the following tuple-data is the key, only for replica identity index configs.
        // 'K' (Optional) - States the following tuple-data is the old tuple, only for replica identity full configs.
        //
        // 'N' (Not-Optional) - States the following tuple-data is the new tuple.
        // This is always present.
        if ('O' == tupleType || 'K' == tupleType) {
            oldData = DecodeUtils.resolveColumnsFromStreamTupleData(buffer, table);
            // Read the 'N' tuple type
            // This is necessary so the stream position is accurate for resolving the column tuple data
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

    /**
     * 处理删除消息
     *
     * @param buffer
     * @return
     */
    private PgOutMessage handDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        char tupleType = (char) buffer.get();
        final Table table = tableCache.get(relationId);
        if (table == null) {
            log.warn("无对应的表信息,relationId:{}", relationId);
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
