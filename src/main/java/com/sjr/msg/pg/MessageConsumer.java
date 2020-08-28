package com.sjr.msg.pg;

import com.google.common.util.concurrent.ListenableFuture;
import com.sjr.msg.message.PgOutMessage;

/**
 * 消费者通用接口
 *
 * @author TMW
 **/
public interface MessageConsumer {

    /***
     * @param messages 消息集合
     * @return 返回future
     * **/
    ListenableFuture<Boolean> process(PgOutMessage messages);
}
