package com.blue.transformation

import com.blue.SparkContext

object Join extends SparkContext with App {

  val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  val b = a.keyBy(_.length)
  val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
  val d = c.keyBy(_.length)
  val result: Array[(Int, (String, String))] = b.join(d).collect
  println(result.mkString(","))
  //  (6,(salmon,salmon)),
  //  (6,(salmon,rabbit)),
  //  (6,(salmon,turkey)),
  //  (6,(salmon,salmon)),
  //  (6,(salmon,rabbit)),
  //  (6,(salmon,turkey)),
  //  (3,(dog,dog)),
  //  (3,(dog,cat)),
  //  (3,(dog,gnu)),
  //  (3,(dog,bee)),
  //  (3,(rat,dog)),
  //  (3,(rat,cat)),
  //  (3,(rat,gnu)),
  //  (3,(rat,bee))
}
