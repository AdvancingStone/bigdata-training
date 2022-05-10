package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Very similar to fold, but performs the folding separately for each key of the RDD.
 * This function is only available if the RDD consists of two-component tuples.
 */
object FoldByKey extends SparkContext with App {

  val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  val b: RDD[(Int, String)] = a.map(x => (x.length, x))
  val result: Array[(Int, String)] = b.foldByKey("")(_ + _).collect
  println(result.mkString(", "))
  //  (3,dogcatowlgnuant)

  val c = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val d = c.map(x => (x.length, x))
  val result2: Array[(Int, String)] = d.foldByKey("")((a, b)=>s"${a}-${b}").collect
  println(result2.mkString(","))
  //  (4,-lion),(3,-dog--cat),(7,-panther),(5,-tiger--eagle)
}
