package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Behaves different from sample in the following respects:
 * -  It will return an exact number of samples (Hint: 2nd parameter) 它将返回准确的样本数（提示：第二个参数）。
 * -  It returns an Array instead of RDD. 它返回一个数组而不是RDD。
 * -  It internally randomizes the order of the items returned. 它在内部随机化返回的项目顺序。
 */
object TakeSample extends SparkContext with App {

  val x = sc.parallelize(1 to 1000, 3)
  private val result: Array[Int] = x.takeSample(true, 10, 1)
  println(result.mkString(","))
  //  630,743,715,404,700,568,822,222,854,586
}
