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
public abstract class PGAbstractConsumer implements MessageConsumer {

    private static final ListeningExecutorService EXECUTOR_SERVICE = MoreExecutors.listeningDecorator(
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(55635),
                    new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("pg-message-consumer-%d")
                            .setUncaughtExceptionHandler((t, e) -> log.error("pg 消息消费异常：{}", e.getMessage()))
                            .setThreadFactory(Thread::new).build()
            )
    );

    @Override
    public ListenableFuture<Boolean> process(PgOutMessage messages) {
        return EXECUTOR_SERVICE.submit(() -> doProcess(messages));
    }

    /**
     * 模板方法
     *
     * @param messages 消息集合
     * @return 是否操作成功
     **/
    protected abstract boolean doProcess(PgOutMessage messages);
}
