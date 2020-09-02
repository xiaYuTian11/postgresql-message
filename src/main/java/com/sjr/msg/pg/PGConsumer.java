package com.sjr.msg.pg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sjr.msg.util.JackSonUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author TMW
 * @date 2020/8/31 9:19
 */
@Slf4j
public class PGConsumer extends AbstractPGConsumer {
    @Override
    protected boolean doProcess(PgOutMessage message) {
        try {
            log.info("消息编号：" + message.getLsnNum());
            log.info(JackSonUtil.JSON.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            log.error("json 转换异常", e);
        }

        // TODO: 2020/8/31 自定义处理消息逻辑
        return true;
    }
}
