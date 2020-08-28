package com.sjr.msg.pg;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sjr.msg.message.PgOutMessage;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 流复制启动
 *
 * @author TMW
 * @date 2020/8/28 17:11
 */
@Slf4j
public class PGReplicationRun {

    private static final ListeningExecutorService EXECUTOR_SERVICE = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("PGReplicationRun-thread-%d").build()
    ));

    /**
     * 异步执行
     *
     * @param connectionConfig
     */
    private synchronized void asyncRun(ConnectionConfig connectionConfig) {
        EXECUTOR_SERVICE.execute(() -> run(connectionConfig));
    }

    /**
     * 同步执行
     *
     * @param config
     */
    public void run(ConnectionConfig config) {
        if (Objects.isNull(config)) {
            log.error("connection is null");
            return;
        }
        if (CollectionUtil.isEmpty(config.getSyncTableSet())) {
            log.error("sync table set is empty");
            return;
        }

        final ConnectionManager manager = ConnectionManager.getInstance();
        manager.init(config);

    }

    /**
     * 接收数据
     */
    public void receive(ConnectionManager manager) {
        while (true) {
            final Optional<ByteBuffer> optional = manager.readPending();
            if (!optional.isPresent()) {
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    log.error("中断异常.");
                }
            } else {
                optional.ifPresent(buffer -> {
                    final PgOutMessage message = OutPutPlugin.getInstance().process(buffer);
                    if (message == null) {
                        return;
                    }
                    final MessageType opt = message.getOpt();
                    if (MessageType.DELETE.equals(opt) || MessageType.INSERT.equals(opt) || MessageType.UPDATE.equals(opt)) {
                        if (MessageType.UPDATE.equals(opt)) {
                            final Set<String> changeKey = message.getChangeKey();
                            if (changeKey == null || changeKey.isEmpty()) {
                                return;
                            }
                        }
                        // process(message);
                    }
                });
            }
        }
    }

}
