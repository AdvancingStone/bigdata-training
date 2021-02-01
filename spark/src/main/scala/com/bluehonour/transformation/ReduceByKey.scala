package com.bluehonour.transformation

import com.bluehonour.SparkContext

object ReduceByKey extends SparkContext with App {

  val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  val b = a.map(x => (x.length, x))
  val result: Array[(Int, String)] = b.reduceByKey(_ + _).collect
  println(result.mkString(","))
  //  (3,dogcatowlgnuant)

  val c = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val d = c.map(x => (x.length, x))
  val result2: Array[(Int, String)] = d.reduceByKey(_ + _).collect
  println(result2.mkString(","))
  //  (4,lion),(3,dogcat),(7,panther),(5,tigereagle)
}
