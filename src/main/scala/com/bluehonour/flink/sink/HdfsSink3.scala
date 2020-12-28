package com.bluehonour.flink.sink

import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.beans.BeanProperty
import scala.collection.immutable.Range.Long

object HdfsSink3 {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "hdfs")
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
      .withBucketAssigner(new UserBucketAssigner())
      .withBucketCheckInterval(60000) // 桶检查间隔，这里设置为60s
      .build()

    restStream.addSink(sink)
    env.execute()
  }


}

case class User(uid: Long, uvalue: String) extends Serializable {
  @BeanProperty var id: Long = uid
  @BeanProperty var value: String = uvalue
}

class UserBucketAssigner extends BucketAssigner[User, String] {
  override def getBucketId(element: User, context: BucketAssigner.Context): String = {
    println(s"element: ${element.id} -> ${element.value}")
    element.id.toString
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    SimpleVersionedStringSerializer.INSTANCE
  }
}