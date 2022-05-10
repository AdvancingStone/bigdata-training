package com.blue.action

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Similar to collect, but works on key-value RDDs and converts them into Scala maps to preserve their key-value structure.
 */
object CollectAsMap extends SparkContext with App {
  val a = sc.parallelize(List(1, 2, 1, 3), 1)
  val b = a.map(_ * 2)
  val c: RDD[(Int, Int)] = a.zip(b)
  val result: collection.Map[Int, Int] = c.collectAsMap()
  println(result)
  //  Map(2 -> 4, 1 -> 2, 3 -> 6)

}
