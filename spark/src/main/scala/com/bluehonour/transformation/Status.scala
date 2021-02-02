package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.util.StatCounter

/**
 * Simultaneously computes the count, mean, the standard deviation, max, min of all values in the RDD.
 * 同时计算RDD中所有值的计数、平均值、标准偏差、最大值、最小值
 */
object Status extends SparkContext with App {
  val x = sc.parallelize(List(1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0), 2)
  private val result: StatCounter = x.stats
  println(result)
  //  (count: 9, mean: 11.266667, stdev: 8.126859, max: 21.000000, min: 1.000000)
}
