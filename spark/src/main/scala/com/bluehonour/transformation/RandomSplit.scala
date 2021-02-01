package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Randomly splits an RDD into multiple smaller RDDs according to a weights Array which specifies the percentage of the total data elements that is assigned to each smaller RDD.
 * Note the actual size of each smaller RDD is only approximately equal to the percentages specified by the weights Array.
 *
 * The second example below shows the number of items in each smaller RDD does not exactly match the weights Array.
 * A random optional seed can be specified.
 * This function is useful for spliting data into a training set and a testing set for machine learning.
 *
 * 根据指定分配给每个较小RDD的数据元素占总数据元素百分比的权重数组，将一个RDD随机分割成多个较小RDD。
 * 请注意，每个较小 RDD 的实际大小仅约等于权重数组指定的百分比。
 *
 * 下面的第二个例子表明，每个较小的 RDD 中的项目数量并不完全匹配权重 Array。
 * 可以指定一个随机的可选种子。
 * 这个函数对于将数据分割成机器学习的训练集和测试集很有用。
 *
 * Listing Variants
 * -  def randomSplit(weights: Array[Double], seed: Long = Utils.random.nextLong): Array[RDD[T]]
 */
object RandomSplit extends SparkContext with App {

  val y = sc.parallelize(1 to 10, 1)
  val splits: Array[RDD[Int]] = y.randomSplit(Array(0.6, 0.4), seed = 10L)
  val training = splits(0)
  val test = splits(1)
  val trainingRes: Array[Int] = training.collect
  println(trainingRes.mkString(","))
  //  1,4,5,6,7,8

  val testRes: Array[Int] = test.collect
  println(testRes.mkString(","))
  //  2,3,9,10

  val x = sc.parallelize(1 to 50)
  val splitsArr: Array[RDD[Int]] = x.randomSplit(Array(0.1, 0, 3, 0.6))
  val rdd1 = splitsArr(0)
  val rdd2 = splitsArr(1)
  val rdd3 = splitsArr(2)
  println(rdd1.collect().mkString(",")) //3,19
  println(rdd2.collect().mkString(",")) //
  println(rdd3.collect().mkString(",")) //1,2,5,6,7,8,9,10,11,13,15,16,17,18,20,21,23,24,25,26,27,28,30,32,33,34,35,37,38,39,40,41,43,44,45,46,49,50

}
