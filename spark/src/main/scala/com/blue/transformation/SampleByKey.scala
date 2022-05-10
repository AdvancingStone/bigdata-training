package com.blue.transformation

import com.blue.SparkContext

/**
 * Randomly samples the key value pair RDD according to the fraction of each key you want to appear in the final RDD.
 */
object SampleByKey extends SparkContext with App {
  val randRDD = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
  val sampleMap: Map[Int, Double] = List((7, 0.4), (6, 0.6)).toMap
  val result: Array[(Int, String)] = randRDD.sampleByKey(false, sampleMap, 42).collect
  println(result.mkString(","))
  //  (7,cat),(6,mouse),(7,tv)
}
