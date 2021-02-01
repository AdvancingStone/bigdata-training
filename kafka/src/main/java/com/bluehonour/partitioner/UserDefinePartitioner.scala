package com.bluehonour.partitioner

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Cluster, PartitionInfo}

class UserDefinePartitioner extends Partitioner{

  private var atomicInteger = new AtomicInteger(0)
  /**
    * 自定义分区器
    * @param topic
    * @param key
    * @param keyBytes
    * @param value
    * @param valueBytes
    * @param cluster
    * @return
    */
  override def partition(topic: String,
                         key: Any, keyBytes: Array[Byte],
                         value: Any, valueBytes: Array[Byte],
                         cluster: Cluster): Int = {
    //获取所有分区
    val partitions: util.List[PartitionInfo] = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size()
    if (keyBytes == null){
      val count = atomicInteger.getAndIncrement()
      return (count & Integer.MAX_VALUE) % numPartitions
    }else{
      // hash the keyBytes to choose a partition
      return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions
    }

  }

  override def close(): Unit = {
    println("close...")
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    println("configure...")
  }
}
