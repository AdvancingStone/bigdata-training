package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Very similar to groupBy, but instead of supplying a function, the key-component of each pair will automatically be presented to the partitioner.
 */
object GroupByKey extends SparkContext with App {
  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
  val b: RDD[(Int, String)] = a.keyBy(_.length)
  val result: Array[(Int, Iterable[String])] = b.groupByKey.collect
  println(result.mkString(","))
  //(4,CompactBuffer(lion)),(6,CompactBuffer(spider)),(3,CompactBuffer(dog, cat)),(5,CompactBuffer(tiger, eagle))
}
