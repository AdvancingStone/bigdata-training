package com.bluehonour.flink.crash_test

import java.lang
import java.util.Properties
import java.util.regex.Pattern

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.{OptionsFactory, RocksDBStateBackend}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.rocksdb.{BlockBasedTableConfig, ColumnFamilyOptions, DBOptions}

object App {
  def main(args: Array[String]): Unit = {
    val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[MyFunction])
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("bootstrap.servers", "master:9092")
    props.put("group.id", "crash_test1")
    val checkpointPath = "hdfs:///user/view/crash_test"

    val stateBackend: RocksDBStateBackend = new RocksDBStateBackend(checkpointPath, true)
    stateBackend.setOptions(new OptionsFactory {
      private final val blockCacheSize = 64 * 1024 * 1024

      override def createDBOptions(currentOptions: DBOptions): DBOptions = {
        currentOptions.setIncreaseParallelism(4).setUseFsync(false)
      }

      override def createColumnOptions(currentOptions: ColumnFamilyOptions): ColumnFamilyOptions = {
        currentOptions.setTableFormatConfig(
          new BlockBasedTableConfig().setBlockCacheSize(blockCacheSize)
            .setBlockSize(128 * 1024) // 128 * 1024
        )
          .setWriteBufferSize(32 * 1024 * 1024) // 32 * 1024 * 1024
          .setMaxWriteBufferNumber(4)
      }
    })
    env.setStateBackend(stateBackend.asInstanceOf[StateBackend])

    // 开启 Checkpoint，每 10秒进行一次 Checkpoint, 语义设置为 EXACTLY_ONCE
    env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE)
    // 当 Flink 任务取消时，保留外部保存的 CheckPoint 信息
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // CheckPoint 的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000);
    // 同一时间，只允许 有 1 个 Checkpoint 在发生
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    // 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500);
    // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true);

    val consumer = new FlinkKafkaConsumer[String]("crash_test1", new SimpleStringSchema(), props)

    consumer.setStartFromLatest()
    consumer.setCommitOffsetsOnCheckpoints(true)

    val kafkaSource: DataStream[String] = env.addSource(consumer)


    val source = kafkaSource.filter(e => {
      log.info(s"===========================   传入的字符串是 ${e}     ========================")
      val arr: Array[String] = e.trim.split("\\s+")
      e != null && !e.trim.equals("") && arr.length == 2 && isNum(arr(0).trim)
    }).map(v => {
      val arr: Array[String] = v.trim.split("\\s+")
      (arr(0).trim.toLong, arr(1).trim)
    })

    val process: DataStream[String] = source.keyBy(_._1)
      .process(new MyFunction)

    process.map(_ + ",map").print()
    process.addSink(new FlinkKafkaProducer[String]("crash_test_result", new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord("crash_test_result", element.getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute("crash test")


  }

  def isNum(str: String): Boolean = {
    val pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    pattern.matcher(str).matches()
  }
}
