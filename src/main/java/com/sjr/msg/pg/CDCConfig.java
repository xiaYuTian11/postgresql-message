package com.sjr.msg.pg;

import com.sjr.msg.mq.PGConsumerToMQProd;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Logical replication
 * <p>
 * <p>
 * https://www.infoq.cn/article/lp5ucrKTI3V4aW1PWvxm
 * https://www.modb.pro/db/11522
 * 中文文档 http://postgres.cn/docs/11/
 *
 * @author TMW
 * @date 2020/8/28 10:15
 */
@Component
public class CDCConfig implements ApplicationRunner {

    @Autowired
    private ConnectionConfig connectionConfig;

    @Override
    public void run(ApplicationArguments args) {
        // 创建
        // final PGReplicationRun pgReplicationRun = new PGReplicationRun(new PGConsumerToMQProd());
        // ConnectionConfig connectionConfig = new ConnectionConfig();
        // connectionConfig.setHost("192.168.3.9");
        // connectionConfig.setPort("5432");
        // connectionConfig.setUserName("postgres");
        // connectionConfig.setPassword("20191809");
        // connectionConfig.setDatabaseName("devdb");
        // connectionConfig.setPublicationName("jyj_publication");
        // connectionConfig.setSlotName("jyj_slot");
        // Set<String> tableSet = new HashSet<>(16);
        // tableSet.add("a01");
        // tableSet.add("a02");
        // tableSet.add("a05");
        // tableSet.add("a06");
        // tableSet.add("a08");
        // tableSet.add("a14");
        // tableSet.add("a15");
        // tableSet.add("a29");
        // tableSet.add("a30");
        // tableSet.add("a36");
        // tableSet.add("a37");
        // tableSet.add("b01");
        // connectionConfig.setSyncTableSet(tableSet);
        new PGReplicationRun(new PGConsumerToMQProd()).asyncRun(connectionConfig);
    }
}
