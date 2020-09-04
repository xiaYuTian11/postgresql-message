package com.sjr.msg.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sjr.msg.pg.AbstractPGConsumer;
import com.sjr.msg.pg.PgOutMessage;
import com.sjr.msg.process.TableFactory;
import com.sjr.msg.process.TableProcess;
import com.sjr.msg.util.JMSUtil;
import com.sjr.msg.util.JMSUtil1;
import com.sjr.msg.util.JackSonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

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
            log.info("接收到数据库数据变化的消息：{}", message.toString());
            final TableProcess tableProcess = TableFactory.createProcess(message.getTableName());
            Optional.ofNullable(tableProcess).flatMap(tbp -> Optional.ofNullable(tbp.process(message))).ifPresent(syncData -> {
                try {
                    final String asString = JackSonUtil.JSON.writeValueAsString(syncData);
                    log.info("消息编号：{}，发送到mq", message.getLsnNum());
                    JMSUtil1.getInstance().sendMessage(JMSUtil.JYJ, asString);
                } catch (JsonProcessingException e) {
                    log.error("解析消息:{}，发送给mq出错：", syncData.toString(), e);
                }
            });
        } catch (Exception e) {
            log.error("处理mq同步消息出错：", e);
        }
        return true;
    }

}
