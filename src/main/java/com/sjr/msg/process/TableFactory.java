package com.sjr.msg.process;

import com.sjr.msg.process.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TMW
 * @date 2020/9/1 9:56
 */
public class TableFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableFactory.class);

    /**
     * 获取同步表数据处理
     *
     * @param tableName
     * @return
     */
    public static TableProcess createProcess(String tableName) {
        final TableType tableType = TableType.forType(tableName);
        if(tableType == null){
            return null;
        }
        switch (tableType) {
            case A01:
                return new A01TableProcess();
            case A02:
                return new A02TableProcess();
            case A05:
                return new A05TableProcess();
            case A06:
                return new A06TableProcess();
            case A08:
                return new A08TableProcess();
            case A14:
                return new A14TableProcess();
            case A15:
                return new A15TableProcess();
            case A29:
                return new A29TableProcess();
            case A30:
                return new A30TableProcess();
            case A36:
                return new A36TableProcess();
            case A37:
                return new A37TableProcess();
            case B01:
                return new B01TableProcess();
            default:
                LOGGER.warn("未找到符合条件的同步表：{} : {}", tableType.getName(), tableType.getDesc());
                return null;
        }
    }

}
