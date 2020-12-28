package com.bluehonour.flink.sink

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.{FSDataOutputStream, Path}
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Preconditions

object HdfsSink2 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("master02", 8888)
    val restStream = stream.filter(data => {
      data!=null && !data.trim.equals("")
    }).map(data=>{
      data.trim
    })

    val sink: StreamingFileSink[String] = StreamingFileSink.forBulkFormat(
      //      new Path("hdfs://master02:9000/tmp/ls"),
      new Path("d:/home/xxx/tmp/rests"),
      new DayBulkWriterFactory
    )
      .withBucketAssigner(new DayBucketAssigner())
      .withBucketCheckInterval(60000) // 桶检查间隔，这里设置为60s
      .build()

    restStream.addSink(sink)
    env.execute()
  }


}

class DayBucketAssigner extends BucketAssigner[String, String] {
  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    val array = element.split("\\s+")
    println(s"element: ${element}")
    println(s"====  array(0) :    ${array(0)}       =======")
    array(0)
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    SimpleVersionedStringSerializer.INSTANCE
  }
}

class DayBulkWriter extends BulkWriter[String] {

  val charset: Charset = StandardCharsets.UTF_8
  var stream: FSDataOutputStream = _

  def dayBulkWriter(inputStream: FSDataOutputStream): DayBulkWriter = {
    stream = Preconditions.checkNotNull(inputStream)
    this
  }

  override def addElement(element: String): Unit = {
    this.stream.write(element.getBytes(charset))
    this.stream.write("\n".getBytes(charset))
  }

  override def flush(): Unit = {
    this.stream.flush()
  }

  override def finish(): Unit = {
    this.flush()
  }
}

class DayBulkWriterFactory extends BulkWriter.Factory[String] {
  override def create(out: FSDataOutputStream): BulkWriter[String] = {
    val bulkWriter: DayBulkWriter = new DayBulkWriter
    bulkWriter.dayBulkWriter(out)
  }
}