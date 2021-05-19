#### kafka 

https://www.cnblogs.com/yoke/p/11536517.html

#### kafka启动

```bash
kafka-server-start.sh -daemon /opt/software/kafka/config/server.properties
```



#### kafka关闭

```bash
kafka-server-stop.sh
```



#### topic

##### topic创建

```bash
kafka-topics.sh 
--zookeeper master:2181,slave1:2181,slave2:2181[/kafka0.11] (这里参考 zookeeper.properties: zookeeper.connect=master:2181,slave1:2181,slave2:2181/kafka0.11)
--create
--topic topocName
--partitions 3
--replication-factor 3
```



##### topic查看

```bash
kafka-topics.sh 
--zookeeper master:2181,slave1:2181,slave2:2181[/kafka0.11] 
--list
```



##### topic详情

```bash
kafka-topics.sh 
--zookeeper master:2181,slave1:2181,slave2:2181[/kafka0.11] 
--describe
--topic topicName
```



##### topic修改

```bash
kafka-topics.sh 
--zookeeper master:2181,slave1:2181.slave2:2181[/kafka0.11]
--alter
--topic topicName
--partitions 4 (注：分区只能增大，不能变小)
```



##### topic删除

```bash
kafka-topics.sh
--zookeeper master:2181,slave1:2181,slave2:2181[/kafka0.11]
--delete
--topic topicName
```



##### topic生产

```bash
kafka-console-producer.sh
--broker-list master:9092,slave1:9092,slave2:9092
--topic topicName
```



##### topic消费

```bash
kafka-console-consumer.sh
--bootstrap-server master:9092,slave1:9092,slave2:9092
--topic topicName
--group group1...
--property print.key=true
--property print.value=true
--property key.separator=,
--from-beginning
```

