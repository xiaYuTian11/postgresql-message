package com.sjr.msg;

import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class MsgApplicationTests {

    @Test
    void contextLoads() {
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        String url = "jdbc:postgresql://139.9.233.106:9095/logic_test";
        Properties props = new Properties();
        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "20191809");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        Connection con = DriverManager.getConnection(url, props);
        PGConnection replConnection = con.unwrap(PGConnection.class);
        Statement statement = con.createStatement();
        // 检查 创建 pg_publication

        // 检查 创建 REPLICA IDENTITY FULL

        // 创建复制插槽
        replConnection.getReplicationAPI().dropReplicationSlot("demo_logical_slot");

        replConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("demo_logical_slot")
                .withOutputPlugin("pgoutput")
                .make();

        // 创建逻辑复制流
        PGReplicationStream stream = replConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("demo_logical_slot")
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                .withStatusInterval(20, TimeUnit.SECONDS)
                .start();

        while (true) {
            //Receive last successfully send to queue message. LSN ordered.
            // LogSequenceNumber successfullySendToQueue = getQueueFeedback();
            // if (successfullySendToQueue != null) {
            //     stream.setAppliedLSN(successfullySendToQueue);
            //     stream.setFlushedLSN(successfullySendToQueue);
            // }

            // System.out.println("connect status:" + con.isClosed());

            //non blocking receive message
            ByteBuffer msg = stream.readPending();

            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

            // try {
            //     final HashMap<String, String> stringStringHashMap = DecodeUtils.resolveColumnsFromStreamTupleData(msg);
            //     stringStringHashMap.forEach((l,v)->{
            //         System.out.println(l);
            //     });
            // } catch (Exception e) {
            //     e.printStackTrace();
            // }

            // System.out.println("------------------");
            // System.out.println((char) msg.get());
            // System.out.println("------------------");
            int offset = msg.arrayOffset();
            byte[] source = msg.array();
            int length = source.length - offset;
            final String s = new String(source, offset, length);
            System.out.println(s);
            //feedback
            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
    }
}
