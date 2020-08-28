// package com.sjr.msg.biz.service;
//
// import com.sjr.msg.biz.dao.A08Mapper;
// import com.sjr.msg.decoder.OutPutDecoder;
// import com.sjr.msg.message.MessageType;
// import com.sjr.msg.message.PgOutMessage;
// import com.sjr.msg.util.JMSUtil;
// import com.sjr.msg.util.JackSonUtil;
// import lombok.extern.slf4j.Slf4j;
// import org.postgresql.PGConnection;
// import org.postgresql.PGProperty;
// import org.postgresql.replication.LogSequenceNumber;
// import org.postgresql.replication.PGReplicationStream;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.stereotype.Service;
//
// import java.nio.ByteBuffer;
// import java.sql.*;
// import java.time.Duration;
// import java.util.HashSet;
// import java.util.Optional;
// import java.util.Properties;
// import java.util.Set;
// import java.util.concurrent.TimeUnit;
//
// /**
//  * select * from  pg_replication_slots  复制槽
//  * SELECT * FROM pg_publication    发布
//  *
//  * @author TMW
//  * @date 2020/8/24 9:40
//  */
// @Service
// @Slf4j
// public class A08Service {
//
//     @Autowired
//     private A08Mapper a08Mapper;
//     private HashSet<String> focus = new HashSet<String>() {{
//         add("a08");
//
//     }};
//     private static final String CREATE_PUBLICATION_SQL_FORMAT = "CREATE PUBLICATION %s FOR TABLE  %s WITH (publish = 'insert,update,delete');";
//     private static final String QUERY_PUBLICATION_SQL_FORMAT = "SELECT count(1) FROM pg_publication where pubname = '%s';";
//     private static final String DROP_PUBLICATION_SQL_FORMAT = "DROP PUBLICATION %s;";
//     private static final String ALTER_TABLE_REPLICA_SQL_FORMAT = "ALTER TABLE \"%s\" REPLICA IDENTITY FULL";
//     private static final String CREATE_PUBLICATION_SLOT_SQL_FORMAT = "CREATE_REPLICATION_SLOT %s %s LOGICAL %s;";
//     private static final String publicationName = "jyj_publication_name";
//     private static final String slotName = "jyj_slot_name";
//     private LogSequenceNumber lastLsn;
//     private PGReplicationStream stream;
//
//     public static void main(String[] args) throws SQLException, InterruptedException {
//         String url = "jdbc:postgresql://47.110.133.228:5432/dev";
//         Properties props = new Properties();
//         PGProperty.USER.set(props, "postgres");
//         PGProperty.PASSWORD.set(props, "123456");
//         PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
//         PGProperty.REPLICATION.set(props, "database");
//         PGProperty.PREFER_QUERY_MODE.set(props, "simple");
//
//         Connection connection = DriverManager.getConnection(url, props);
//         PGConnection replConnection = connection.unwrap(PGConnection.class);
//
//         // replConnection.getReplicationAPI().dropReplicationSlot("demo_logical_slot");
//         A08Service a08Service = new A08Service();
//         boolean isOk = false;
//
//         log.info("create publication if not exist.");
//         isOk = a08Service.createPublication(connection);
//         if (!isOk) {
//             throw new RuntimeException("创建数据库订阅异常!请检查数据库连接配置.");
//         }
//         log.info("alter table REPLICA.");
//         isOk = a08Service.alterTableReplica(connection);
//         if (!isOk) {
//             throw new RuntimeException("修改表数据订阅方式异常!");
//         }
//         log.info("create publication slot.");
//         isOk = a08Service.createPublicationSlot(connection);
//         if (!isOk) {
//             throw new RuntimeException("创建订阅槽失败!");
//         }
//
//         log.info("get table primary key info.");
//         isOk = a08Service.getPkInfo(connection);
//         if (!isOk) {
//             throw new RuntimeException("获取主键信息失败!");
//         }
//
//         log.info("make publication stream.");
//         isOk = a08Service.makePGStream(connection);
//         if (!isOk) {
//             throw new RuntimeException("获取流失败!");
//         }
//
//         while (true) {
//             ByteBuffer byteBuffer = a08Service.stream.readPending();
//             System.out.println(a08Service.stream.getLastReceiveLSN());
//             Optional<ByteBuffer> optional = Optional.ofNullable(byteBuffer);
//             optional.ifPresent(buffer -> {
//                 final PgOutMessage message = OutPutDecoder.getInstance().process(buffer);
//                 if (message == null) {
//                     return;
//                 }
//                 final MessageType opt = message.getOpt();
//                 if (MessageType.DELETE.equals(opt) || MessageType.INSERT.equals(opt) || MessageType.UPDATE.equals(opt)) {
//                     if (MessageType.UPDATE.equals(opt)) {
//                         final Set<String> changeKey = message.getChangeKey();
//                         if (changeKey == null || changeKey.isEmpty()) {
//                             return;
//                         }
//                     }
//
//                     JMSUtil.sendMessage("a08", JackSonUtil.toJson(message));
//                 }
//             });
//         }
//     }
//
//     /**
//      * 创建订阅
//      *
//      * @param connection 连接对象
//      * @return 是否创建成功
//      **/
//     private boolean createPublication(Connection connection) {
//         final HashSet<String> focus = this.focus;
//         if (focus == null || focus.isEmpty()) {
//             log.warn("focus table is empty.");
//             return false;
//         }
//         final String queryPublicationSql = String.format(QUERY_PUBLICATION_SQL_FORMAT, publicationName);
//         boolean isExist = false;
//         try (
//                 final Statement statement = connection.createStatement();
//                 final ResultSet resultSet = statement.executeQuery(queryPublicationSql)
//         ) {
//             final boolean next = resultSet.next();
//             if (!next) {
//                 return false;
//             }
//             final long count = resultSet.getLong(1);
//             log.info("Sql: {}", queryPublicationSql);
//             if (count == 0L) {
//                 log.info("publication name {} is not exist! will be create {} for publication", publicationName, publicationName);
//             } else {
//                 isExist = true;
//                 log.info("publication name {} is already exist!", publicationName);
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//             log.error("sql 执行异常!", e);
//             return false;
//         }
//
//         if (isExist) {
//             return true;
//         }
//
//         try (final Statement statement = connection.createStatement()) {
//             final String createSql = String.format(CREATE_PUBLICATION_SQL_FORMAT, publicationName, this.getPublicationTablesSql());
//             statement.execute(createSql);
//             log.info("Sql: {}", createSql);
//             log.info("create publication {} success.", publicationName);
//
//             return true;
//
//         } catch (SQLException e) {
//             e.printStackTrace();
//             log.error("create publication {} error.", publicationName);
//             return false;
//         }
//     }
//
//     public String getPublicationTablesSql() {
//         if (focus.isEmpty()) {
//             return null;
//         }
//         StringBuilder builder = new StringBuilder();
//         focus.forEach(tableName -> {
//             builder.append("\"");
//             builder.append(tableName);
//             builder.append("\"");
//             builder.append(",");
//         });
//         final String sql = builder.toString();
//         return sql.substring(0, sql.length() - 1);
//     }
//
//     /***
//      * 修改表数据订阅方式
//      * @param connection 连接
//      * @return 是否修改成功.
//      * **/
//     private boolean alterTableReplica(Connection connection) {
//         final HashSet<String> focus = this.focus;
//         if (focus.isEmpty()) {
//             log.error("focus table is empty.");
//             return false;
//         }
//         try (final Statement statement = connection.createStatement()) {
//             for (String tableName : focus) {
//                 final String sql = String.format(ALTER_TABLE_REPLICA_SQL_FORMAT, tableName);
//                 log.info("Sql: {}", sql);
//                 statement.execute(sql);
//             }
//             return true;
//         } catch (SQLException e) {
//             e.printStackTrace();
//             log.error("修改订阅表异常!", e);
//             return false;
//         }
//     }
//
//     /***
//      * 创建订阅槽
//      * @param connection 数据库连接
//      * @return 是否创建成功
//      * **/
//     private boolean createPublicationSlot(Connection connection) {
//         try (Statement statement = connection.createStatement()) {
//             final String sql = String.format(CREATE_PUBLICATION_SLOT_SQL_FORMAT, slotName, "TEMPORARY", "pgoutput");
//             log.info("Sql:{}", sql);
//             final boolean isOk = statement.execute(sql);
//             if (!isOk) {
//                 log.error("创建订阅槽失败!");
//                 return false;
//             }
//             final ResultSet resultSet = statement.getResultSet();
//             final boolean next = resultSet.next();
//             if (!next) {
//                 resultSet.close();
//                 log.error("创建订阅槽无返回信息!");
//                 return false;
//             }
//             String startPoint = resultSet.getString("consistent_point");
//             lastLsn = LogSequenceNumber.valueOf(LogSequenceNumber.valueOf(startPoint).asLong());
//             resultSet.close();
//         } catch (Exception exception) {
//             exception.printStackTrace();
//             log.error("创建订阅槽失败!", exception);
//             return false;
//         }
//         return true;
//     }
//
//     /***
//      * 获取关注表的主键信息
//      * **/
//     private boolean getPkInfo(Connection connection) {
//         try {
//             final DatabaseMetaData metaData = connection.getMetaData();
//             for (String tableName : focus) {
//                 try (ResultSet resultSet = metaData.getPrimaryKeys(connection.getCatalog(), "public", tableName)) {
//                     resultSet.next();
//                     final String pkName = resultSet.getString("COLUMN_NAME");
//                     OutPutDecoder.getInstance().addPkCache(tableName, pkName);
//                 } catch (SQLException e) {
//                     log.error("获取表信息失败.", e);
//                     return false;
//                 }
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//             log.error("获取表信息失败.", e);
//             return false;
//         }
//         return true;
//     }
//
//     /****
//      * @param connection 数据库连接
//      * @return 是否创建成功
//      * **/
//     private boolean makePGStream(Connection connection) {
//         try {
//             final PGConnection pgConnection = connection.unwrap(PGConnection.class);
//             this.stream = pgConnection.getReplicationAPI()
//                     .replicationStream()
//                     .logical()
//                     .withSlotName(slotName)
//                     .withSlotOption("proto_version", 1)
//                     .withSlotOption("publication_names", publicationName)
//                     .withStartPosition(lastLsn)
//                     .withStatusInterval(Math.toIntExact(Duration.ofSeconds(10).toMillis()), TimeUnit.MILLISECONDS)
//                     .start();
//             stream.forceUpdateStatus();
//             return true;
//         } catch (SQLException e) {
//             e.printStackTrace();
//             log.error("创建订阅流失败!", e);
//         }
//         return false;
//     }
// }
