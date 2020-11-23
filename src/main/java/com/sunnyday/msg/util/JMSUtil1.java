package com.sunnyday.msg.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.AsyncCallback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 消息工具类
 *
 * @author TMW
 * @date 2020/8/26 15:13
 */
@Slf4j
@Component
public class JMSUtil1 {

    public static ConnectionFactory connectionFactory;
    public static Connection connection = null;
    public static Session session;
    public static Map<String, MessageProducer> sendQueues = new ConcurrentHashMap<>();
    public static Map<String, MessageConsumer> receiveQueues = new ConcurrentHashMap<>();
    public static final AtomicBoolean IS_INIT = new AtomicBoolean(Boolean.FALSE);
    public static JMSUtil1 JMS_UTIL;

    private static String USER_NAME;
    private static String PASSWORD;
    public static String QUEUE_NAME;
    private static String HOST;
    private static String PORT;
    private static final String DEFAULT_URL = "failover://(tcp://%S:%S)";

    @Value(("${activemq.username}"))
    public void setUserName(String userName) {
        JMSUtil1.USER_NAME = userName;
    }

    @Value(("${activemq.password}"))
    public void setPassword(String password) {
        JMSUtil1.PASSWORD = password;
    }

    @Value(("${activemq.queue_name}"))
    public void setQueueName(String queueName) {
        JMSUtil1.QUEUE_NAME = queueName;
    }

    @Value(("${activemq.host}"))
    public void setHost(String host) {
        JMSUtil1.HOST = host;
    }

    @Value(("${activemq.port}"))
    public void setPort(String port) {
        JMSUtil1.PORT = port;
    }

    private JMSUtil1() {

    }

    /**
     * 获取单例
     *
     * @return
     */
    public synchronized static JMSUtil1 getInstance() {
        if (JMS_UTIL == null) {
            JMS_UTIL = new JMSUtil1();
            JMS_UTIL.init();
            return JMS_UTIL;
        }
        return JMS_UTIL;
    }

    /**
     * 初始化
     */
    public void init() {
        if (!IS_INIT.get()) {
            log.info("初始化 ActiveMQ 连接");
            connectionFactory = new ActiveMQConnectionFactory(
                    USER_NAME,
                    PASSWORD,
                    getUrl());
            try {
                connection = connectionFactory.createConnection();
                // 异步消息传递
                ((ActiveMQConnection) connection).setUseAsyncSend(true);
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (Exception e) {
                log.error("初始化 ActiveMQ 错误：", e);
            }
            IS_INIT.compareAndSet(false, true);
        }
    }

    /**
     * 获取连接
     *
     * @return
     */
    private String getUrl() {
        return String.format(DEFAULT_URL, HOST, PORT);
    }

    /**
     * 获取生产者
     *
     * @param queueName 队列名称
     * @return
     */
    private MessageProducer getMessageProducer(String queueName) {
        if (sendQueues.containsKey(queueName)) {
            return sendQueues.get(queueName);
        }
        try {
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            // queue 模式默认是可持久化
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
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
    private MessageConsumer getMessageConsumer(String queueName) {
        if (receiveQueues.containsKey(queueName)) {
            return receiveQueues.get(queueName);
        }
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            receiveQueues.put(queueName, consumer);
            return consumer;
        } catch (JMSException e) {
            log.error("获取 mq 消费者异常", e);
        }
        return receiveQueues.get(queueName);
    }

    /**
     * 发送消息
     *
     * @param queue
     * @param text
     */
    public void sendMessage(String queue, String text) {
        try {
            final MessageProducer producer = getMessageProducer(queue);
            TextMessage message = session.createTextMessage(text);
            // 异步发送消息回调
            ((ActiveMQMessageProducer) producer).send(message, new AsyncCallback() {
                @Override
                public void onSuccess() {
                    log.info("sendMessage to {} : {}", queue, text);
                }

                @Override
                public void onException(JMSException exception) {
                    boolean flag;
                    for (int i = 0; i < 5; i++) {
                        log.error("sendMessage to {} 错误,进行重试", queue);
                        try {
                            producer.send(message);
                            flag = true;
                        } catch (JMSException e) {
                            log.error("sendMessage to {} with {} 重试错误,消息未正确发送", queue, text);
                            flag = false;
                        }
                        if (flag) {
                            break;
                        } else {
                            try {
                                TimeUnit.SECONDS.sleep(5);
                            } catch (InterruptedException e) {
                                log.error("重发消息休眠异常", e);
                            }
                        }
                    }
                }
            });
        } catch (JMSException e) {
            log.error("发送消息异常", e);
        }
    }

    /**
     * 接收消息
     *
     * @param queue
     * @return
     */
    public String receiveMessage(String queue) {
        try {
            final Message receive = getMessageConsumer(queue).receive(1000 * 60 * 3);
            receive.acknowledge();
            TextMessage message = (TextMessage) receive;
            return message.getText();
        } catch (JMSException e) {
            log.error("接收消息异常", e);
        }
        return null;
    }

    /**
     * 异步接收消息
     *
     * @param queue
     * @param consumer
     */
    public void receiveMessage(String queue, Consumer<Message> consumer) throws JMSException {
        getMessageConsumer(queue).setMessageListener(consumer::accept);
    }

    public static void close() {
        try {
            session.close();
        } catch (JMSException e) {
            log.error("关闭 mq session  异常", e);
        }
        try {
            connection.close();
        } catch (JMSException e) {
            log.error("关闭 mq connection 异常", e);
        }
    }
}
