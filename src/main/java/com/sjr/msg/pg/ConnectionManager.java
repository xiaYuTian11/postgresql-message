package com.sjr.msg.pg;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;

import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 连接管理
 * https://github.com/xiaYuTian11/debezium
 *
 * @author TMW
 * @date 2020/8/28 11:32
 */
@Slf4j
public class ConnectionManager {
    /**
     * 连接管理是咧
     */
    private static ConnectionManager CONNECT_MANAGER;
    /**
     * 创建 订阅
     */
    private static final String CREATE_PUBLICATION_SQL_FORMAT = "CREATE PUBLICATION %s FOR TABLE  %s WITH (publish = 'insert,update,delete');";
    /**
     * 查询订阅
     */
    private static final String QUERY_PUBLICATION_SQL_FORMAT = "SELECT count(1) FROM pg_publication where pubname = '%s';";
    /**
     * 删除订阅
     */
    private static final String DROP_PUBLICATION_SQL_FORMAT = "DROP PUBLICATION %s;";
    /**
     * 更正同步表数据方式
     */
    private static final String ALTER_TABLE_REPLICA_SQL_FORMAT = "ALTER TABLE \"%s\" REPLICA IDENTITY FULL";
    /**
     * 创建管道
     */
    private static final String CREATE_PUBLICATION_SLOT_SQL_FORMAT = "CREATE_REPLICATION_SLOT %s %s LOGICAL %s;";
    /**
     * 日志序列编号
     */
    private AtomicReference<LogSequenceNumber> lastLsn = new AtomicReference<>();
    /**
     * 逻辑流
     */
    private AtomicReference<PGReplicationStream> stream = new AtomicReference<>();
    /**
     * JDBC连接
     */
    private AtomicReference<Connection> connection = new AtomicReference<>();
    /**
     * 连接配置
     */
    private AtomicReference<ConnectionConfig> config = new AtomicReference<>();
    /**
     * 是否成功
     */
    private final AtomicBoolean isError = new AtomicBoolean(Boolean.FALSE);
    private final AtomicBoolean isRunning = new AtomicBoolean(Boolean.TRUE);

    /**
     * 获取对象单列
     *
     * @return
     */
    public static synchronized ConnectionManager getInstance() {
        if (CONNECT_MANAGER == null) {
            CONNECT_MANAGER = new ConnectionManager();
        }
        return CONNECT_MANAGER;
    }

    /**
     * 初始化相关配置
     *
     * @return
     */
    public void init(ConnectionConfig connectionConfig) {
        config.set(connectionConfig);

        log.info("create pgsql connection!");
        createConnection();
        if (isError.get()) {
            throw new RuntimeException("创建数据库连接异常!请检查数据库连接配置.");
        }

        log.info("create publication if not exist.");
        createPublication();
        if (isError.get()) {
            throw new RuntimeException("创建数据库订阅异常!请检查数据库连接配置.");
        }

        log.info("reset table REPLICA IDENTITY.");
        resetTableReplica();
        if (isError.get()) {
            throw new RuntimeException("修改表数据订阅方式异常!");
        }

        log.info("create publication slot.");
        createPublicationSlot();
        if (isError.get()) {
            throw new RuntimeException("创建订阅槽失败!");
        }
        log.info("get table primary key info.");
        getPkInfo();
        if (isError.get()) {
            throw new RuntimeException("获取主键信息失败!");
        }
        log.info("make publication stream.");
        replicationStream();
        if (isError.get()) {
            throw new RuntimeException("获取流失败!");
        }
    }

    /**
     * 创建 JDBC 连接
     */
    private void createConnection() {
        try {
            connection.set(DriverManager.getConnection(config.get().getJdbcUrl(), config.get().getProperties()));
        } catch (SQLException e) {
            log.error("获取 JDBC connection 错误：", e);
            isError.set(Boolean.TRUE);
        }
    }

    /**
     * 创建订阅
     */
    private void createPublication() {
        final String queryPublicationSql = String.format(QUERY_PUBLICATION_SQL_FORMAT, config.get().getPublicationName());
        log.info("query publication sql: {}", queryPublicationSql);
        try (
                final Statement statement = connection.get().createStatement();
                final ResultSet resultSet = statement.executeQuery(queryPublicationSql)
        ) {
            if (resultSet.next()) {
                final long count = resultSet.getLong(1);
                if (count == 0L) {
                    try {
                        log.info("publication name {} is not exist! will be create {} for publication", config.get().getPublicationName(), config.get().getPublicationName());
                        final String createSql = String.format(CREATE_PUBLICATION_SQL_FORMAT, config.get().getPublicationName(), config.get().getPublicationTablesSql());
                        statement.execute(createSql);
                        log.info("create publication sql: {}", createSql);
                    } catch (SQLException e) {
                        log.error("创建 publication {} 错误: ", config.get().getPublicationName(), e);
                        isError.set(true);
                    }
                } else {
                    log.info("publication name {} is already exist!", config.get().getPublicationName());
                }
            }
        } catch (SQLException e) {
            log.error("检查 publication 是否存在是错误：", e);
            isError.set(true);
        }
    }

    /**
     * 重置表数据订阅方式
     */
    private void resetTableReplica() {
        final Set<String> syncTableSet = config.get().getSyncTableSet();
        try (final Statement statement = connection.get().createStatement()) {
            for (String tableName : syncTableSet) {
                final String sql = String.format(ALTER_TABLE_REPLICA_SQL_FORMAT, tableName);
                statement.execute(sql);
                log.info("alter {} REPLICA IDENTITY to set full, Sql: {}", tableName, sql);
            }
        } catch (SQLException e) {
            log.error("修改订阅表异常!", e);
            isError.set(true);
        }
    }

    /**
     * 创建复制槽
     */
    private void createPublicationSlot() {
        final String slotName = config.get().getSlotName();
        final String lifeCycle = config.get().getSlotLifeCycle();
        final String pluginName = config.get().getOutputPluginName();
        final String sql = String.format(CREATE_PUBLICATION_SLOT_SQL_FORMAT, slotName, lifeCycle, pluginName);
        try (
                Statement statement = connection.get().createStatement();
                final ResultSet resultSet = statement.executeQuery(sql)
        ) {
            log.info("CREATE_REPLICATION_SLOT Sql:{}", sql);

            if (!resultSet.next()) {
                resultSet.close();
                log.error("创建订阅槽无返回信息!");
                isError.set(true);
                return;
            }
            String startPoint = resultSet.getString("consistent_point");
            lastLsn.set(LogSequenceNumber.valueOf(LogSequenceNumber.valueOf(startPoint).asLong()));
        } catch (Exception e) {
            log.error("创建订阅槽失败!", e);
            isError.set(true);
        }
    }

    /**
     * 获取关注表的主键信息
     */
    private void getPkInfo() {
        try {
            final DatabaseMetaData metaData = connection.get().getMetaData();
            for (String tableName : config.get().getSyncTableSet()) {
                try (ResultSet resultSet = metaData.getPrimaryKeys(connection.get().getCatalog(), "public", tableName)) {
                    if (resultSet.next()) {
                        final String pkName = resultSet.getString("COLUMN_NAME");
                        OutPutPlugin.getInstance().addPkCache(tableName, pkName);
                    }

                } catch (SQLException e) {
                    log.error("获取表信息失败.", e);
                    isError.set(true);
                }
            }
        } catch (SQLException e) {
            log.error("获取表信息失败.", e);
            isError.set(true);
        }
    }

    /**
     * 创建逻辑复制流
     * https://jdbc.postgresql.org/documentation/head/replication.html
     */
    private void replicationStream() {
        try {
            final PGConnection pgConnection = connection.get().unwrap(PGConnection.class);
            final PGReplicationStream replicationStream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(config.get().getSlotName())
                    .withSlotOption("proto_version", 1)
                    .withSlotOption("publication_names", config.get().getPublicationName())
                    .withStartPosition(lastLsn.get())
                    .withStatusInterval(Math.toIntExact(Duration.ofSeconds(10).toMillis()), TimeUnit.MILLISECONDS)
                    .start();
            replicationStream.forceUpdateStatus();
            stream.set(replicationStream);
        } catch (SQLException e) {
            log.error("创建订阅流失败!", e);
            isError.set(true);
        }
    }

    /***
     * 读取数据
     * @return buff
     * **/
    public Optional<ByteBuffer> readPending() {
        if (isRunning.get()) {
            try {
                return Optional.ofNullable(stream.get().readPending());
            } catch (Exception e) {
                log.warn("读取流信息出错：", e);
                flushConnection();
                return Optional.empty();
            }
        } else {
            flushConnection();
        }
        return Optional.empty();
    }

    /**
     * 获取lsn
     *
     * @return
     */
    public LogSequenceNumber getLastLsn() {
        return stream.get().getLastReceiveLSN();
    }

    /**
     * 设置消息位置
     *
     * @param lsn 消息编号
     **/
    public void setStreamLsn(LogSequenceNumber lsn) throws SQLException {
        if (isRunning.get()) {
            try {
                stream.get().setAppliedLSN(lsn);
                stream.get().setFlushedLSN(lsn);
                stream.get().forceUpdateStatus();
            } catch (PSQLException e) {
                log.error("stream error: ", e);
                isRunning.set(false);
            }
        }
    }

    /**
     * 关闭资源
     */
    public void shutdown() {
        if (stream.get() != null) {
            try {
                log.info("close the stream.");
                stream.get().close();
            } catch (SQLException e) {
                log.error("关闭数据库流异常.", e);
            }
        }
        if (connection.get() != null) {
            try {
                log.info("close the connection.");
                connection.get().close();
            } catch (SQLException e) {
                log.error("关闭数据库连接异常.", e);
            }
        }
    }

    /**
     * 重新连接
     */
    public void flushConnection() {
        try {
            log.warn("数据库连接异常,10秒后进行数据库重连.");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException interruptedException) {
                log.error("数据库重连失败", interruptedException);
            }
            isRunning.set(false);
            // if (stream.get() != null && !stream.get().isClosed()) {
            //     stream.get().close();
            // }

            if (connection.get() != null && !connection.get().isClosed()) {
                connection.get().close();
            }

            createConnection();
            if (connection.get() == null) {
                return;
            }

            createPublicationSlot();
            replicationStream();
            isRunning.set(true);
            log.info("重连数据库成功");
        } catch (Exception e) {
            log.error("重连数据库失败，10秒钟进行重试", e);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException interruptedException) {
                log.error("重连数据库失败，休眠异常", interruptedException);
            }
        }
    }

}
