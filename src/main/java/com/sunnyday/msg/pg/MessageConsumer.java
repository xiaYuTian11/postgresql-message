package com.sunnyday.msg.pg;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * 消费者通用接口
 *
 * @author TMW
 **/
public interface MessageConsumer {

    /***
     * @param message 消息集合
     * @return 返回future
     * **/
    ListenableFuture<Boolean> process(PgOutMessage message);
}
