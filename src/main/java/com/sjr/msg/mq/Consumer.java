package com.sjr.msg.mq;

import com.sjr.msg.pg.PgOutMessage;
import com.sjr.msg.util.JMSUtil;
import com.sjr.msg.util.JackSonUtil;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * 消费者
 *
 * @author TMW
 * @date 2020/8/26 11:43
 */
public class Consumer {

    public static void main(String[] args) throws JMSException {
        // while (true) {
        //     System.out.println("接收消息......");
        //     final String test = JMSUtil.receiveMessage("jyj");
        //     if (!Strings.isNullOrEmpty(test)) {
        //         final PgOutMessage pgOutMessage = JackSonUtil.toObject(test, PgOutMessage.class);
        //         System.out.println(pgOutMessage);
        //     }
        // }

        while (true) {
            JMSUtil.receiveMessage("jyj", message -> {
                TextMessage textMessage = (TextMessage) message;
                if (textMessage != null) {
                    try {
                        final String text = textMessage.getText();
                        final PgOutMessage pgOutMessage = JackSonUtil.toObject(text, PgOutMessage.class);
                        System.out.println("------------------------------------------------------------");
                        System.out.println(pgOutMessage);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

    }

}
