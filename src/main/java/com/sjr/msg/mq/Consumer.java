package com.sjr.msg.mq;

import com.sjr.msg.entity.SyncData;
import com.sjr.msg.util.JMSUtil;
import com.sjr.msg.util.JackSonUtil;
import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.text.SimpleDateFormat;

/**
 * 消费者
 *
 * @author TMW
 * @date 2020/8/26 11:43
 */
@Slf4j
public class Consumer {

    public static void main(String[] args) throws JMSException {
        while (true) {
            JMSUtil.receiveMessage("jyj", message -> {
                if (message != null) {
                    try {
                        // message.acknowledge();
                        TextMessage textMessage = (TextMessage) message;
                        final String text = textMessage.getText();
                        final SyncData syncData = JackSonUtil.toObject(text, SyncData.class);
                        if (syncData == null) {
                            log.warn("接收消息为空");
                            return;
                        }
                        log.info("接收消息：{}", syncData.toString());
                    } catch (Exception e) {
                        log.error("接收消息错误：", e);
                    }
                } else {
                    log.warn("接收消息为空");
                }
            });
        }
    }

}

@Slf4j
class JyjMessageListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
        // JMSUtil.getMessageConsumer("jyj").setMessageListener(new JyjMessageListener());
        log.info("-------------------");
        if (message != null) {
            try {
                // message.acknowledge();
                TextMessage textMessage = (TextMessage) message;
                final String text = textMessage.getText();
                final SyncData syncData = JackSonUtil.toObject(text, SyncData.class);
                log.info("接收消息：{}", syncData.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            log.info("message is null");
        }
    }
}
