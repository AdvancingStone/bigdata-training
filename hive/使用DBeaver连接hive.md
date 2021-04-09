使用DBeaver连接hive



在core-site.xml配置

```xml
<property>
	<name>hive.server2.thrift.client.user</name>
	<value>hadoop</value>
	<description>Username to use against thrift client. default is 'anonymous'</description>
</property>
<property>
	<name>hive.server2.thrift.client.password</name>
	<value>hadoop</value>
	<description>Password to use against thrift client. default is 'anonymous'</description>
</property>

```

配置hive-site.xml

```xml
<!-- 指定 hiveserver2 连接的 host -->
<property>
	<name>hive.server2.thrift.bind.host</name>
	<value>master</value>
</property>
<!-- 指定 hiveserver2 连接的端口号 默认10000 -->
<property>
	<name>hive.server2.thrift.port</name>
	<value>10000</value>
</property>

```



下载两个jar包

```sh
# 先安装rz
yum install -y rz
# 下载
sz /opt/software/hive/lib/hive-jdbc-1.2.1-standalone.jar
sz /opt/software/hadoop/share/hadoop/common/hadoop-common-2.7.4.jar
```

将以上俩jar配置到DBeaver

开启metastore 和 hiveserver2

```sh
nohup hive --service metastore >> /opt/software/hive/logs/metastore.log 2>&1 &
nohup  hive --service hiveserver2 >> /opt/software/hive/logs/hiveserver2.log 2>&1 &
```

