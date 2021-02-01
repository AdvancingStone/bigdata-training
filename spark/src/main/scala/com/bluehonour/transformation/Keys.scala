package com.bluehonour.transformation

import com.bluehonour.SparkContext

object Keys extends SparkContext with App {
  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b = a.map(x => (x.length, x))
  val result: Array[Int] = b.keys.collect
  println(result.mkString(","))
  //  3,5,4,3,7,5
}
