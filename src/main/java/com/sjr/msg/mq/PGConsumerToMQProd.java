package com.sjr.msg.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sjr.msg.pg.AbstractPGConsumer;
import com.sjr.msg.pg.PgOutMessage;
import com.sjr.msg.util.JMSUtil;
import com.sjr.msg.util.JackSonUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 消费 pgsql 流消息，解析后数据作为 mq 提供方
 *
 * @author TMW
 * @date 2020/8/26 11:33
 */
@Slf4j
public class PGConsumerToMQProd extends AbstractPGConsumer {

    @Override
    protected boolean doProcess(PgOutMessage message) {
        try {
            log.info("消息编号：" + message.getLsnNum());
            final String msgStr = JackSonUtil.JSON.writeValueAsString(message);
            log.info(msgStr);
            JMSUtil.sendMessage("jyj", msgStr);
        } catch (JsonProcessingException e) {
            log.error("解析消息，发送给mq出错：", e);
        }
        return true;
    }
}
