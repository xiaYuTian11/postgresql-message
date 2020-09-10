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
 
    
   
