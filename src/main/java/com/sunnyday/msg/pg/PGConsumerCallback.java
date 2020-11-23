package com.sunnyday.msg.pg;

import com.google.common.util.concurrent.FutureCallback;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.SQLException;

/**
 * 回调对象
 */
@Slf4j
public class PGConsumerCallback implements FutureCallback<Boolean> {

    private LogSequenceNumber lastLsn;

    public PGConsumerCallback(LogSequenceNumber lastLsn) {
        this.lastLsn = lastLsn;
    }

    @Override
    public void onSuccess(@Nullable Boolean result) {
        if (result != null && result) {
            try {
                ConnectionManager.getInstance().setStreamLsn(lastLsn);
                log.info("message {} successful!", lastLsn.asString());
            } catch (SQLException e) {
                log.error("message " + lastLsn.asString() + "消费失败!", e);
            }
            return;
        }
        log.info("消息编号为:{} 消费失败,后续操作将会一直消费该消息,直到成功为止..", lastLsn.asString());
    }

    @Override
    public void onFailure(Throwable t) {
        log.error("回调失败,异常.", t);
    }
}
