package com.sjr.msg;

import com.sjr.msg.util.JMSUtil;

/**
 * 消费者
 *
 * @author TMW
 * @date 2020/8/26 11:43
 */
public class Consumer {
    // public static void main(String[] args) throws JMSException {
    //     ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("admin", "123456", "tcp://47.110.133.228:61616");
    //     final Connection connection = factory.createConnection();
    //     final Session session = connection.createSession(Boolean.TRUE, Session.CLIENT_ACKNOWLEDGE);
    //     connection.start();
    //     final Queue queue = session.createQueue("jyj");
    //     final MessageConsumer consumer = session.createConsumer(queue);
    //     // TextMessage message = (TextMessage) consumer.receive();
    //     // final Message receive = consumer.receive();
    //     //         // final String body = receive.getBody(String.class);
    //     // System.out.println("receive message: " + message.getText());
    //     consumer.setMessageListener(new MessageListener() {
    //         @SneakyThrows
    //         @Override
    //         public void onMessage(Message message) {
    //             message.acknowledge();
    //             TextMessage msg = (TextMessage) message;
    //             try {
    //                 System.out.println("handle message: " + msg.getText());
    //                 // session.close();
    //             } catch (JMSException e) {
    //                 e.printStackTrace();
    //             }
    //         }
    //     });
    //     while (true) {
    //         // session.commit();
    //         // System.out.println("111111111111");
    //     }
    //
    //     // session.close();
    //     // connection.close();
    // }

    public static void main(String[] args) {
        while (true) {
            // final String test = JMSUtil.receiveMessage("a08");
            final String test = JMSUtil.receiveMessage("test");
            System.out.println("receive message ： " + test);
            // if (!Strings.isNullOrEmpty(test)) {
            //     final PgOutMessage pgOutMessage = JackSonUtil.toObject(test, PgOutMessage.class);
            //     System.out.println(pgOutMessage);
            // }
        }
    }

}
