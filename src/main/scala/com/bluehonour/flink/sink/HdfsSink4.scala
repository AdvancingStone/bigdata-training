package com.bluehonour.flink.sink

import java.io.IOException
import java.nio.charset.StandardCharsets

import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import scala.beans.BeanProperty
import scala.collection.immutable.Range.Long

object HdfsSink4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master02", 8888)
    val restStream = stream.filter(data => {
      data != null && !data.trim.equals("")
    }).map(data => {
      val split = data.trim.split("\\s+")
      val user = User(split.head.toLong, split.tail.mkString(", "))
      println(user.id, user.value)
      user
    })

    val sink: StreamingFileSink[User] = StreamingFileSink.forBulkFormat(
//      new Path("hdfs://master02:9000/tmp/ls"),
            new Path("d:/home/xxx/tmp/rests"),
       ParquetAvroCompressionWriters.forReflectRecord(classOf[User], CompressionCodecName.SNAPPY)
    )
      .withBucketAssigner(new UserBucketAssigner2())
      .withBucketCheckInterval(60000) // 桶检查间隔，这里设置为60s
      .build()

    restStream.addSink(sink)
    env.execute()
  }


}


class UserBucketAssigner2 extends BucketAssigner[User, String] {
  override def getBucketId(element: User, context: BucketAssigner.Context): String = {
    println(s"user id: ${element.getId}")
    element.getId.toString
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    StringSerializer
  }
  object StringSerializer extends SimpleVersionedSerializer[String] {
    val VERSION = 77

    override def getVersion = 77

    @throws[IOException]
    override def serialize(checkpointData: String): Array[Byte] = checkpointData.getBytes(StandardCharsets.UTF_8)

    @throws[IOException]
    override def deserialize(version: Int, serialized: Array[Byte]): String = if (version != 77) throw new IOException("version mismatch")
    else new String(serialized, StandardCharsets.UTF_8)
  }
}