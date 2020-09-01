package com.sjr.msg.process;

import cn.hutool.core.collection.CollectionUtil;
import com.sjr.msg.entity.SyncData;
import com.sjr.msg.pg.PgOutMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author TMW
 * @date 2020/9/1 9:59
 */
public interface TableProcess {
    /**
     * 处理数据
     *
     * @param message
     * @return
     */
    default SyncData process(PgOutMessage message) {
        Set<String> syncFields = getSyncFields();
        Set<String> changeKey = message.getChangeKey();
        boolean containsAny = CollectionUtil.containsAny(syncFields, changeKey);
        if (containsAny) {
            Map<String, String> newData = message.getNewData();
            Map<String, String> oldData = message.getOldData();
            // 需要同步的集合
            Map<String, String> syncNewData = new HashMap<>(16);
            Map<String, String> syncOldData = new HashMap<>(16);

            changeKey.forEach(str -> {
                if (syncFields.contains(str)) {
                    syncNewData.put(str, newData.get(str));
                    syncOldData.put(str, oldData.get(str));
                }
            });

            SyncData syncData = new SyncData();
            syncData.setOptCode(message.getOpt().getCode());
            syncData.setTableName(message.getTableName());
            syncData.setPkValue(message.getPkValue());
            syncData.setPkName(message.getPkValue());
            syncData.setNewData(syncNewData);
            syncData.setOldData(syncOldData);
            return syncData;
        }
        return null;
    }

    /**
     * 需要同步的字段
     *
     * @return
     */
    Set<String> getSyncFields();
}
