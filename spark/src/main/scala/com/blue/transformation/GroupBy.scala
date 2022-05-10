package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

object GroupBy extends SparkContext with App {
  val a = sc.parallelize(1 to 9, 3)
  val result1: Array[(String, Iterable[Int])] = a.groupBy(x => if (x % 2 == 0) "even" else "odd").collect
  println(result1.mkString(","))
  //(even,CompactBuffer(2, 4, 6, 8)),(odd,CompactBuffer(1, 3, 5, 7, 9))

  val p = new MyPartitioner(3)
  val b: RDD[(Int, Iterable[Int])] = a.groupBy((x:Int) => x, p)
  val result: Array[String] = b.mapPartitionsWithIndex((index, iter) => {
    iter.map(x => s"index: $index, value: $x")
  }).collect
  println(result.mkString(","))
  // index: 0, value: (6,CompactBuffer(6)),
  // index: 0, value: (3,CompactBuffer(3)),
  // index: 0, value: (9,CompactBuffer(9)),
  // index: 1, value: (4,CompactBuffer(4)),
  // index: 1, value: (1,CompactBuffer(1)),
  // index: 1, value: (7,CompactBuffer(7)),
  // index: 2, value: (8,CompactBuffer(8)),
  // index: 2, value: (5,CompactBuffer(5)),
  // index: 2, value: (2,CompactBuffer(2))
}

import org.apache.spark.Partitioner

class MyPartitioner extends Partitioner {
  var num: Int = _

  def this(num: Int) {
    this
    this.num = num
  }

  override def numPartitions: Int = {
    num
  }

  override def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case key: Int => key % numPartitions
      case _ => key.hashCode() % numPartitions
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case h: MyPartitioner => true
      case _ => false
    }
  }
}