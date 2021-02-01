package com.bluehonour.action

import com.bluehonour.SparkContext
import org.apache.spark.partial.{BoundedDouble, PartialResult}

/**
 * Calls stats and extracts the mean component.
 * The approximate version of the function can finish somewhat faster in some scenarios.
 * However, it trades accuracy for speed.
 *
 * Listing Variants
 * -  def mean(): Double
 * -  def meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
 */
object Mean extends SparkContext with App {

  val a = sc.parallelize(1 to 10, 3)
  val result: Double = a.mean
  println(result) //5.5

  val result2: PartialResult[BoundedDouble] = a.meanApprox(200, 0.95)
  println(result2)  //(final: [5.500, 5.500])
}
