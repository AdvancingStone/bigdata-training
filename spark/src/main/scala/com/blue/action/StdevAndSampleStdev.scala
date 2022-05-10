package com.blue.action

import com.blue.SparkContext

/**
 * Calls stats and extracts either stdev-component or corrected sampleStdev-component.
 * 调用 stats 并提取 标准差组件stdev-component 或修正后的 样本标准差组件sampleStdev-component。
 */
object StdevAndSampleStdev extends SparkContext with App {
  val d = sc.parallelize(List(0.0, 1.0), 3)
  val res1: Double = d.stdev()
  val res2: Double = d.sampleStdev()

  val a = sc.parallelize(List(0.0, 0.0, 1.0), 3)
  val res3: Double = a.stdev()
  val res4: Double = a.sampleStdev()
  println(res1) //0.5
  println(res2) //0.7071067811865476
  println(res3) //0.4714045207910317
  println(res4) //0.5773502691896257
}
