# postgresql-message
  利用 PostgreSQL 新特性 PostgreSQL Logical Replication (逻辑复制) 及 ActiveMQ 完成消息同步发送。
  详情请查看[博客](https://xiayutian11.github.io/)
  
  
deploy.sh 在 Windows 上可以运行，在linux上运行错误：
   ```
        yum install -y dos2unix
        dos2unix deploy.sh
   ```

运行deploy.sh 脚本不需要在前面写 sh
    ```
        chmod +x /usr/.../deploy.sh
    ```

## linux 启动时启动 Spring Boot jar

1.编写启动脚本，此处名为 deploy.sh
2.给 deploy.sh 文件添加执行权限
```shell script
    chmod +x /usr/soft/sync/deploy.sh
```
3.给/etc/rc.d/rc.local文件添加执行权限
```shell script
    chmod +x /etc/rc.d/rc.local
```
4.在 /etc/rc.d/rc.local 文件末尾添加启动脚本命令
```shell script
su - root -c '/usr/soft/sync/deploy.sh start'
```
5.重启服务器验证脚本是否自启动成功。
 
## springboot logback.xml 读取 yml 文件配置问题
由于Springboot 加载顺序是 logback.xml -> application.yml -> logback-spring.xml,所以在logback.xml中是不能读取到yml文件中的配置的
解决方案：
1.在yml 文件中配置日志存放路径：
```yaml
log:
  file:
    path: ./logs
```
2.将logback.xml名称改为 logback-spring.xml
3.在logback-spring.xml 加入spring配置
```xml
<springProperty scope="context" name="logPath" source="log.file.path" defaultValue="logs"/>
```
4.使用配置,部署的时候可在yml 文件中配置绝对路径
```xml
<!-- 定义日志文件的存储地址 勿在 LogBack 的配置中使用相对路径 -->
<property name="LOG_PATH" value="${logPath}"/>
```

## Springboot jar 和 资源分离打包后启动加载 lib及资源
官方文档地址 https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#executable-jar-launching
```shell script
java -jar -Xms512m -Xmx2048m -Dloader.path=lib,resources test.jar
```

   
