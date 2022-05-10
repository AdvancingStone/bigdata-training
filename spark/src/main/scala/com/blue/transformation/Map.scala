package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Applies a transformation function on each item of the RDD and returns the result as a new RDD.
 */
object Map extends SparkContext with App {
  val rdd: RDD[String] = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"))
  val lenRdd = rdd.map(_.length)
  val result = rdd.zip(lenRdd)
  result.collect.foreach(println)
  //(dog,3)
  //(salmon,6)
  //(salmon,6)
  //(rat,3)
  //(elephant,8)
}
