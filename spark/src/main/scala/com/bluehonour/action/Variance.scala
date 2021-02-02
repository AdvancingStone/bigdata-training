package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Calls stats and extracts either variance-component or corrected sampleVariance-component.
 * 调用统计学并提取方差分量或校正后的样本方差分量。
 */
object Variance extends SparkContext with App {

  val a = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5), 3)
  val res1: Double = a.variance
  println(res1)
  //  10.605333333333332

  val x = sc.parallelize(List(1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0), 2)
  val res2: Double = x.variance
  println(res2)
  //  66.04584444444443

  val res3: Double = x.sampleVariance
  println(res3)
  //  74.30157499999999

}
