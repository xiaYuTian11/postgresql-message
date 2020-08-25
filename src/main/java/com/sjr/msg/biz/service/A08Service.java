package com.sjr.msg.biz.service;

import com.google.errorprone.annotations.Var;
import com.sjr.msg.biz.dao.A08Mapper;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author TMW
 * @date 2020/8/24 9:40
 */
@Service
public class A08Service {

    @Autowired
    private A08Mapper a08Mapper;

    public static void main(String[] args) throws SQLException, InterruptedException {
        String url = "jdbc:postgresql://192.168.3.9:5432/devdb";
        Properties props = new Properties();
        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "20191809");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        Connection con = DriverManager.getConnection(url, props);
        PGConnection replConnection = con.unwrap(PGConnection.class);

        // 检查 创建 pg_publication

        // 检查 创建 REPLICA IDENTITY FULL


        // 创建复制插槽
        // replConnection.getReplicationAPI().dropReplicationSlot("demo_logical_slot");
        //
        // replConnection.getReplicationAPI()
        //         .createReplicationSlot()
        //         .logical()
        //         .withSlotName("demo_logical_slot")
        //         .withOutputPlugin("test_decoding")
        //         .make();

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
