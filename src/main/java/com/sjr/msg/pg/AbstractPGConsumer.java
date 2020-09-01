package com.sjr.msg.pg;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 消费者
 *
 * @author TMW
 **/
@Slf4j
public abstract class AbstractPGConsumer implements MessageConsumer {

    private static final ListeningExecutorService EXECUTOR_SERVICE = MoreExecutors.listeningDecorator(
            new ThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(55635),
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("pg-message-consumer-%d")
                            .setUncaughtExceptionHandler((t, e) -> log.error("pg 消息消费异常：", e))
                            .setThreadFactory(Thread::new).build()
            )
    );

    /**
     * 处理消息
     *
     * @param message 消息集合
     * @return
     */
    @Override
    public ListenableFuture<Boolean> process(PgOutMessage message) {
        return EXECUTOR_SERVICE.submit(() -> doProcess(message));
    }

    /**
     * 模板方法
     *
     * @return 是否操作成功
     **/
    protected abstract boolean doProcess(PgOutMessage message);
}
