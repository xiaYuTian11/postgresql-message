package com.sjr.msg.mq;

import com.sjr.msg.entity.SyncData;
import com.sjr.msg.util.JMSUtil;
import com.sjr.msg.util.JackSonUtil;
import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

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
            // log.info("监听消息。");
            // JMSUtil.receiveMessage("jyj", message -> {
            //     if (message != null) {
            //         try {
            //             // message.acknowledge();
            //             TextMessage textMessage = (TextMessage) message;
            //             final String text = textMessage.getText();
            //             final SyncData syncData = JackSonUtil.toObject(text, SyncData.class);
            //             SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //             System.out.println("--------------------" + simpleDateFormat.format(new Date()) + "----------------------");
            //             System.out.println(syncData);
            //         } catch (Exception e) {
            //             e.printStackTrace();
            //         }
            //     }
            // });
            JMSUtil.getMessageConsumer("jyj").setMessageListener(new JyjMessageListener());
        }
    }

}

@Slf4j
class JyjMessageListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
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
