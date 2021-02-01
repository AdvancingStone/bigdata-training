package com.bluehonour.sink

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {
      override def isEndOfStream(nextElement: (String, String)): Boolean = false

      override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(record.key(), "utf-8")
        val value = new String(record.value(), "utf-8")
        (key, value)
      }

      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props))

    val restStream = stream.map(data=>{
      val value = data._2
      val splits = value.split("\t")
      val monitorId = splits(0)
      (monitorId, 1)
    }).keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2+value2._2)
        }
      }).map(x=>x._1 + "\t" +x._2)

    //设置文件滚动策略
    val rolling: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      //当文件超过2s没有写入数据，则滚动产生一个小文件
      .withInactivityInterval(2000)
      //文件打开时间超过2s，则滚动产生一个小文件，每隔2s产生一个小文件
      .withRolloverInterval(2000)
      .withMaxPartSize(256*1024*1024)
      .build()

    /**
     * 默认：
     * 每一个小时对应一个桶（文件夹）， 每一个thread处理的结果对应桶下面的一个小文件
     * 当小文件大小超过128M或者小文件打开时间超过60s，滚动产生第二个小文件
     */

    val sink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(
      new Path("d:/home/xxx/tmp/rests"),
      new SimpleStringEncoder[String]("utf-8")
    ) .withBucketCheckInterval(1000)
      .withRollingPolicy(rolling)
      .build()

    restStream.addSink(sink)
    env.execute()
  }

}
