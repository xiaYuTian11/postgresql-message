package com.sjr.msg.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.AsyncCallback;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 消息工具类
 * http://activemq.apache.org/connection-configuration-uri
 * http://activemq.apache.org/redelivery-policy
 * http://activemq.apache.org/configuring-transports
 *
 * @author TMW
 * @date 2020/8/26 15:13
 */
@Slf4j
public class JMSUtil {

    private static final String USER_NAME = "admin";
    private static final String PASSWORD = "123456";
    // private static final String DEFAULT_URL = "tcp://47.110.133.228:61616";
    /**
     * jms.useAsyncSend=true&jms.producerWindowSize=10240  异步配置  文件过于大也会触发异步
     */
    private static final String DEFAULT_URL = "failover://(tcp://47.110.133.228:61616)&jms.useAsyncSend=true&jms.producerWindowSize=10240&initialReconnectDelay=1000&maxReconnectDelay=15000&jms.redeliveryPolicy.maximumRedeliveries=-1";
    // private static final String DEFAULT_URL = "failover://(tcp://47.110.133.228:61616?wireFormat.maxInactivityDuration=0)&jms.useAsyncSend=true&jms.producerWindowSize=10240&randomize=false&initialReconnectDelay=100&timeout=2000";

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
            // 异步消息传递
            ((ActiveMQConnection) connection).setUseAsyncSend(true);
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
    public static void sendMessage(String queue, String text) {
        try {
            TextMessage message = session.createTextMessage(text);
            final MessageProducer producer = getMessageProducer(queue);
            // 异步发送消息回调
            ((ActiveMQMessageProducer) producer).send(message, new AsyncCallback() {
                @Override
                public void onSuccess() {
                    log.info(" sendMessage to {} : {}", queue, text);
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
    public static String receiveMessage(String queue) {
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
    public static void receiveMessage(String queue, Consumer<Message> consumer) throws JMSException {
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
