package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Very similar to subtract, but instead of supplying a function, the key-component of each pair will be automatically used as criterion for removing items from the first RDD.
 */
object SubtractByKey extends SparkContext with App {
  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
  val b: RDD[(Int, String)] = a.keyBy(_.length)
  val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
  val d: RDD[(Int, String)] = c.keyBy(_.length)
  println(b.collect.mkString(","))
  //  (3,dog),(5,tiger),(4,lion),(3,cat),(6,spider),(5,eagle)
  println(d.collect.mkString(","))
  //  (3,ant),(6,falcon),(5,squid)
  val e: RDD[(Int, String)] = b.subtractByKey(d)
  val result: Array[(Int, String)] = e.collect
  println(result.mkString(","))
  //  (4,lion)
}
