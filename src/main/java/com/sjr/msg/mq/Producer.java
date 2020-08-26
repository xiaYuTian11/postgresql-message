package com.sjr.msg.mq;

import com.sjr.msg.util.JMSUtil;

/**
 * 生产者
 *
 * @author TMW
 * @date 2020/8/26 11:33
 */
public class Producer {

    // public static void main(String[] args) throws JMSException {
    //     ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("admin", "123456", "tcp://47.110.133.228:61616");
    //     final Connection connection = factory.createConnection();
    //     connection.start();
    //     // 使用Connection创建session,第一个参数是是否使用事务，第二个参数是确认机制
    //     final Session session = connection.createSession(Boolean.TRUE, Session.CLIENT_ACKNOWLEDGE);
    //     final Queue queue = session.createQueue("jyj");
    //     final MessageProducer producer = session.createProducer(queue);
    //     final TextMessage textMessage = session.createTextMessage("hahhah");
    //     producer.send(textMessage);
    //     System.out.println("send message: " + textMessage.getText());
    //     session.commit();
    //     session.close();
    //     connection.close();
    // }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            JMSUtil.sendMessage("test", "message" + i);
        }
        JMSUtil.close();
    }
}
