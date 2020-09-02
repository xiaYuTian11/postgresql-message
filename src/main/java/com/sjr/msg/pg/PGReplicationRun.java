package com.sjr.msg.pg;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.util.concurrent.*;
import com.sjr.msg.util.JMSUtil;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.LogSequenceNumber;

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
    private static final ListeningExecutorService EXEC_CALLBACK = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("PGReplicationRun-callback-thread-%d").build()));

    private AbstractPGConsumer consumer;

    public PGReplicationRun(AbstractPGConsumer consumer) {
        this.consumer = consumer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ConnectionManager.getInstance().shutdown();
            JMSUtil.close();
            EXEC_CALLBACK.shutdown();
        }));
    }

    /**
     * 异步执行
     *
     * @param connectionConfig
     */
    public synchronized void asyncRun(ConnectionConfig connectionConfig) {
        EXECUTOR_SERVICE.execute(() -> run(connectionConfig));
    }

    /**
     * 同步执行
     *
     * @param config
     */
    private void run(ConnectionConfig config) {
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
        log.info("Create Logical Replication Success!");
        receive(manager);
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
                        final LogSequenceNumber lastLsn = ConnectionManager.getInstance().getLastLsn();
                        doPGConsumption(message, lastLsn);
                    }
                });
            }
        }
    }

    /**
     * 消费
     *
     * @param message
     * @param lastLsn
     */
    private void doPGConsumption(PgOutMessage message, LogSequenceNumber lastLsn) {
        final ListenableFuture<Boolean> future = consumer.process(message);
        //回调
        Futures.addCallback(future, new PGConsumerCallback(lastLsn), EXEC_CALLBACK);
    }

}
