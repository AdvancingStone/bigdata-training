package com.bluehonour.action

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 该函数将RDD[K,V]中每个K对应的V值根据映射函数来运算，运算结果映射到一个Map[K,V]中，而不是RDD[K,V]。
 */
object ReduceByKeyLocally extends SparkContext with App {
  val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3), ("a", 2)))
  val result: collection.Map[String, Int] = rdd.reduceByKeyLocally(_ + _)
  println(result)
  //  Map(a -> 3, b -> 2, c -> 3)
}
