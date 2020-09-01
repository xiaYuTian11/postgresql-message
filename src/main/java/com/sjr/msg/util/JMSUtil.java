package com.sjr.msg.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 消息工具类
 *
 * @author TMW
 * @date 2020/8/26 15:13
 */
@Slf4j
public class JMSUtil {

    private static final String USER_NAME = "admin";
    private static final String PASSWORD = "123456";
    // private static final String DEFAULT_URL = "tcp://47.110.133.228:61616";
    private static final String DEFAULT_URL = "failover://(tcp://47.110.133.228:61616?wireFormat.maxInactivityDuration=0)&randomize=false&initialReconnectDelay=100&timeout=2000";

    public static final String JYJ = "jyj";

    public static ConnectionFactory connectionFactory;
    public static Connection connection = null;
    public static Session session;
    public static Map<String, MessageProducer> sendQueues = new ConcurrentHashMap<>();
    public static Map<String, MessageConsumer> receiveQueues = new ConcurrentHashMap<>();

    static {
        connectionFactory = new ActiveMQConnectionFactory(
                USER_NAME,
                PASSWORD,
                DEFAULT_URL);
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            // session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (Exception e) {
            log.error("初始化 ActiveMQ 错误：", e);
        }
    }

    /**
     * 获取生产者
     *
     * @param queueName 队列名称
     * @return
     */
    public static MessageProducer getMessageProducer(String queueName) {
        if (sendQueues.containsKey(queueName)) {
            return sendQueues.get(queueName);
        }
        try {
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            sendQueues.put(queueName, producer);
            return producer;
        } catch (JMSException e) {
            log.error("create producer error: ", e);
        }
        return sendQueues.get(queueName);
    }

    /**
     * 获取消费者队列
     *
     * @param queueName
     * @return
     */
    public static MessageConsumer getMessageConsumer(String queueName) {
        if (receiveQueues.containsKey(queueName)) {
            return receiveQueues.get(queueName);
        }
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            receiveQueues.put(queueName, consumer);
            return consumer;
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return receiveQueues.get(queueName);
    }

    /**
     * 发送消息
     *
     * @param queue
     * @param text
     */
    public static void sendMessage(String queue, String text) {
        try {
            TextMessage message = session.createTextMessage(text);
            getMessageProducer(queue).send(message);
            log.info(" sendMessage to {} : {}", queue, text);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * 接收消息
     *
     * @param queue
     * @return
     */
    public static String receiveMessage(String queue) {
        try {
            final Message receive = getMessageConsumer(queue).receive(1000 * 60 * 3);
            receive.acknowledge();
            TextMessage message = (TextMessage) receive;
            if (message != null) {
                return message.getText();
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 异步接收消息
     *
     * @param queue
     * @param consumer
     */
    public static void receiveMessage(String queue, Consumer<Message> consumer) throws JMSException {
        getMessageConsumer(queue).setMessageListener(message -> {
            try {
                message.acknowledge();
                consumer.accept(message);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
    }

    public static void close() {
        try {
            session.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
