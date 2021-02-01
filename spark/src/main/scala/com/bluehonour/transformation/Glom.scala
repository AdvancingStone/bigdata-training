package com.bluehonour.transformation

import org.apache.spark.rdd.RDD

/**
 * Assembles an array that contains all elements of the partition and embeds it in an RDD.
 * Each returned array contains the contents of one partition.
 */
object Glom extends SparkContext with App {
  val value: RDD[Int] = sc.parallelize(1 to 10, 3)
  val arrRdd: Array[Array[Int]] = value.glom
    .collect()
  val result = arrRdd.deep.mkString("\n")
  println(result)
  //  Array(1, 2, 3)
  //  Array(4, 5, 6)
  //  Array(7, 8, 9, 10)
}
