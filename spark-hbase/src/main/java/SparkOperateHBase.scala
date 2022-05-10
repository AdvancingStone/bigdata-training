import com.blue.constant.Constant.{FAMILY, QUALIFIER, TABLE_NAME, VALUE}
import com.blue.util.{HBaseUtil, KafkaUtil}
import org.apache.hadoop.hbase.{Cell, CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Connection, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jettison.json.JSONObject

import java.util.Properties
import scala.collection.mutable

object SparkOperateHBase {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hconf = HBaseUtil.getHBaseConf
    sc.setLogLevel("ERROR")

    val scan = new Scan()
    val filter = new SingleColumnValueFilter(
      Bytes.toBytes(FAMILY),
      Bytes.toBytes(QUALIFIER),
      CompareOp.EQUAL,
      Bytes.toBytes(VALUE))
    scan.setFilter(filter)

    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)

    hconf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME)
    hconf.set(TableInputFormat.SCAN, scanToString)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count = hbaseRdd.count()
    println(count)


    val dataRowsRdd: JavaPairRDD[String, mutable.HashMap[String, String]] = hbaseRdd.mapToPair(new PairFunction[Tuple2[ImmutableBytesWritable, Result], String, mutable.HashMap[String, String]] {
      override def call(t: (ImmutableBytesWritable, Result)): (String, mutable.HashMap[String, String]) = {
        val result: Result = t._2
        val resultMap = new mutable.HashMap[String, String]()
        for (cell <- result.rawCells()) {
          resultMap.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(), new String(CellUtil.cloneValue(cell)))
        }
        val tuple = Tuple2(Bytes.toString(result.getRow), resultMap)
        tuple
      }
    })



    var count1 = 0
    // RDD数据操作
    val dataRdd = hbaseRdd.foreachPartition(x => {
      val kafka = new KafkaUtil[String, String](kafkaProducerProps)
      x.foreach(y => {
        val conn: Connection = HBaseUtil.getHBaseConn
        val htable = conn.getTable(TableName.valueOf(TABLE_NAME))
        val value = y._2
        val rowkey = Bytes.toString(value.getRow)
        val memberId = Bytes.toString(value.getValue("cf".getBytes, "member_id".getBytes))
        val memberUuid = Bytes.toString(value.getValue("cf".getBytes, "member_uuid".getBytes))
        val transactionAmount = Bytes.toString(value.getValue("cf".getBytes, "transaction_amount".getBytes))
        val timestamps = Bytes.toString(value.getValue("cf".getBytes, "timestamps".getBytes))
        val nextStartDt = Bytes.toString(value.getValue("cf".getBytes, "nx_start".getBytes))
        val nextEndDt = Bytes.toString(value.getValue("cf".getBytes, "nx_end".getBytes))
        val nextType = Bytes.toString(value.getValue("cf".getBytes, "nx_type".getBytes))
        val isPush = Bytes.toString(value.getValue("cf".getBytes, "is_push".getBytes))
        val date = Bytes.toString(value.getValue("cf".getBytes, "date".getBytes))
        val resend = Bytes.toString(value.getValue("cf".getBytes, "resend".getBytes))

        val pushJsonStr = generatePurchaseSendCouponJsonStr(
          memberUuid, timestamps, nextType, nextStartDt, nextEndDt
        )

        println(pushJsonStr)
//        kafka.sendMessagesSync("test", null, pushJsonStr)
        count1 = count1 + 1
        println(s"kafka发送了$count1 条")

        HBaseUtil.addRow(htable, rowkey, FAMILY, "is_push", "1")
      })
      kafka.closeKafkaProducer()
    }

    )
    sc.stop()
    val endTime = System.currentTimeMillis()
    println(s"总耗时: ${endTime - startTime}")
  }


  def generatePurchaseSendCouponJsonStr(userId: String, timestamp: String, `type`: String, startDt: String, endDt: String): String = {
    val json = new JSONObject
    json.put("from", "DMP")
    json.put("type", `type`)
    json.put("userId", userId)
    json.put("timestamp", timestamp)
    val messJson = new JSONObject
    messJson.put("startDt", startDt)
    messJson.put("endDt", endDt)
    json.put("message", messJson)
    json.toString()
  }

  //    val hBaseContext: HBaseContext = new HBaseContext(sc, hconf)
  //    val pairRdd: RDD[(ImmutableBytesWritable, Result)] = hBaseContext.hbaseRDD(TableName.valueOf(table), scan)
  //    val tuples: Array[(ImmutableBytesWritable, Result)] = pairRdd.take(10)
  //    tuples.foreach(tuple => {
  //      val row: ImmutableBytesWritable = tuple._1
  //      println(row.get())
  //      val result: Result = tuple._2
  //
  //      val cells = result.listCells().asScala
  //      cells.foreach(cell => {
  //        val bytes: Array[Byte] = CellUtil.cloneValue(cell)
  //        val result = Bytes.toString(bytes)
  //        println(result)
  //      })
  //    })

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave1:9092,slave2:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    //设置kafka的acks和retries
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    //不包含第一次发送，如果系统尝试三次失败，则系统放弃发送
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    //将检测时间设置为10ms
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10")
    //开启kafka的幂等性
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props
  }

}
