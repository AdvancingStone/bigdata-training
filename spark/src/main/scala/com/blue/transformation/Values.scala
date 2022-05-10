package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

object Values extends SparkContext with App {

  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b: RDD[(Int, String)] = a.map(x => (x.length, x))
  val c: RDD[(Int, String)] = a.keyBy(_.length)
  val res1: Array[String] = b.values.collect
  val res2: Array[String] = c.values.collect
  println(res1.mkString(","))
  println(res2.mkString(","))
  //  dog,tiger,lion,cat,panther,eagle
}
