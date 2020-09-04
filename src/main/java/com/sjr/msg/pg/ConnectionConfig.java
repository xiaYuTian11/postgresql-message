package com.sjr.msg.pg;

import lombok.Data;
import org.postgresql.PGProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * pgsql 配置
 * <p>
 * https://jdbc.postgresql.org/documentation/head/replication.html
 *
 * @author TMW
 * @date 2020/8/28 11:14
 */
@Data
@ConfigurationProperties("pgsql")
@Configuration
public class ConnectionConfig {

    private static final String JDBC_URL_FORMAT = "jdbc:postgresql://%s:%s/%s?tcpKeepAlive=true";
    private static final String DEFAULT_SLOT_NAME = "default_slot";
    private static final String DEFAULT_PUBLICATION_NAME = "default_publication";

    public ConnectionConfig() {

    }

    /**
     * ip
     */
    private String host;
    /**
     * 端口
     */
    private String port;
    /**
     * 用户名
     */
    private String userName;
    /**
     * 密码
     */
    private String password;
    /**
     * 数据库
     */
    private String databaseName;
    /**
     * 订阅号名称
     */
    private String publicationName = DEFAULT_PUBLICATION_NAME;
    /**
     * 复制槽名称
     */
    private String slotName = DEFAULT_SLOT_NAME;
    /**
     * 解析流插件名称
     */
    private String outputPluginName = "pgoutput";
    /**
     * 复制槽存活周期
     */
    private String slotLifeCycle = "TEMPORARY";

    /**
     * 需要同步的表名称
     */
    private Set<String> syncTableSet;

    /**
     * 数据库连接url
     *
     * @return jdbc url
     **/
    public String getJdbcUrl() {
        return String.format(JDBC_URL_FORMAT, this.getHost(), this.getPort(), this.getDatabaseName());
    }

    /***
     * 数据库连接配置
     * @return properties
     * **/
    public Properties getProperties() {
        Properties props = new Properties();
        PGProperty.USER.set(props, getUserName());
        PGProperty.PASSWORD.set(props, getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return props;
    }

    /**
     * 获取需要同步的表
     *
     * @return
     */
    public String getPublicationTablesSql() {
        return syncTableSet.stream().map(tableName -> "\"" + tableName + "\"").collect(Collectors.joining(","));
    }
}
