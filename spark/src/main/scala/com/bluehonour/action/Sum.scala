package com.bluehonour.action

import com.bluehonour.SparkContext
import org.apache.spark.partial.{BoundedDouble, PartialResult}

/**
 * Computes the sum of all values contained in the RDD.
 * The approximate version of the function can finish somewhat faster in some scenarios. However, it trades accuracy for speed.
 */
object Sum extends SparkContext with App {
  val x = sc.parallelize(List(1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0), 2)
  val result1: Double = x.sum
  println(result1)
  //  101.39999999999999

  val result2: PartialResult[BoundedDouble] = x.sumApprox(10, 0.95)
  println(result2.getFinalValue())
  //  [101.400, 101.400]
}

