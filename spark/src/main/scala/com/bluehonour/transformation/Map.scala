package com.bluehonour.transformation

import org.apache.spark.rdd.RDD

object Map extends SparkContext {
  def main(args: Array[String]): Unit = {
    val rdd: RDD[String] = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"))
    val lenRdd = rdd.map(_.length)
    val result = rdd.zip(lenRdd)
    result.collect.foreach(println)
  }
}
