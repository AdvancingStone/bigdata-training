package com.blue.action

import com.blue.SparkContext
import org.apache.spark.partial.{BoundedDouble, PartialResult}

object CountByKey extends SparkContext with  App {

  val c = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
  val result: collection.Map[Int, Long] = c.countByKey
  println(result)
  //  Map(3 -> 3, 5 -> 1)

  val result2: PartialResult[collection.Map[Int, BoundedDouble]] = c.countByKeyApprox(2000, 0.95)
  println(result2)
  //  (final: Map(3 -> [3.000, 3.000], 5 -> [1.000, 1.000]))
}
