package com.bluehonour.action

import com.bluehonour.transformation.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 聚合函数允许用户对 RDD 应用两种不同的reduce函数。
 * 第一个reduce函数在每个分区内应用，将每个分区内的数据reduce成一个结果。
 * 第二个reduce函数用于将所有分区的不同reduce结果合并在一起，以得出一个最终结果。
 * 拥有两个独立的reduce函数用于分区内reduce与跨分区reduce的能力增加了很多灵活性。
 * 例如，第一个reduce函数可以是最大函数，第二个可以是和函数。用户还可以指定一个初始值。以下是一些重要的事实。
 *
 * 1. 初始值在两个层次的reduce中都被应用。因此，在分区内reduce和跨分区reduce时都会应用初始值。
 * 2. 两个reduce函数都必须是换算的和关联的。
 * 3. 无论是分区计算还是合并分区，都不要假设任何执行顺序。
 * 4. 为什么要使用两种输入数据类型呢？假设我们用金属探测器做一个考古遗址调查。在走过遗址的时候，我们根据金属探测器的输出，采集重要发现的GPS坐标。
 * 之后，我们打算用 aggregate 函数绘制一张地图的图像，突出这些位置。在这种情况下，zeroValue可以是一个没有亮点的区域图。
 * 可能庞大的输入数据集以GPS坐标的形式存储在许多分区中，seqOp(第一个第二个reducer)可以将GPS坐标转换为地图坐标，
 * 并在地图上的相应位置放上一个标记，combOp(第二个reducer)将以局部地图的形式接收这些亮点，并将它们组合成一个单一的最终输出地图。
 */
object aggregate extends SparkContext with App {

  // lets first print out the contents of the RDD with partition labels
  def myFunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
    iter.map(x => "[partID:" + index + ", val: " + x + "]")
  }

  val z: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
  val collect: Array[String] = z.mapPartitionsWithIndex(myFunc).collect
  println(collect.mkString(","))
  // [partID:0, val: 1],[partID:0, val: 2],[partID:1, val: 3],[partID:1, val: 4],[partID:2, val: 5],[partID:2, val: 6]

  // reduce of partition 0 will bi max(0, 1, 2) = 2
  // reduce of partition 1 will bi max(0, 3, 4) = 4
  // reduce of partition 2 will bi max(0, 5, 6) = 6
  // final reduce across partitions will be 2 + 4 + 6 = 12
  val result: Int = z.aggregate(0)(math.max(_, _), _ + _)
  println(result) // 12

  // reduce of partition 0 will bi max(5, 1, 2) = 5
  // reduce of partition 1 will bi max(5, 3, 4) = 5
  // reduce of partition 2 will bi max(5, 5, 6) = 6
  // final reduce across partitions will be 5 + 5 + 5 + 6 = 21
  // note the final reduce include the initial value 5
  val result2: Int = z.aggregate(5)(math.max(_, _), _ + _)
  println(result2)

  //===========================================
  //lets first print out the contents of the RDD with partition labels
  def myfunc2(index: Int, iter: Iterator[(String)]): Iterator[String] = {
    iter.map(x => s"[partID: ${index}, val: ${x}]")
  }

  val a = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)
  val collect1: Array[String] = a.mapPartitionsWithIndex(myfunc2).collect
  println(collect1.mkString(","))
  //  [partID: 0, val: a],[partID: 0, val: b],[partID: 0, val: c],[partID: 1, val: d],[partID: 1, val: e],[partID: 1, val: f]

  val str: String = a.aggregate("")(_ + _, _ + _)
  println(str) //abcdef

  // See here how the initial value "x" is applied three times.
  //  - once for each partition
  //  - once when combining all the partitions in the second reduce function.
  val str1: String = a.aggregate("@")(_ + _, _ + _)
  println(str1) //@@abc@def or @@def@abc

  // Below are some more advanced examples. Some are quite tricky to work out.
  val b = sc.parallelize(List("12", "23", "345", "4567"), 2)
  val str2: String = b.aggregate("")((x, y) => math.max(x.length, y.length).toString, (x, y) => x + y)
  println(str2) //24 or 42

  val c = sc.parallelize(List("12", "23", "345", "1"), 2)
  val str3: String = c.aggregate("")(
    (x, y) => {
      val s: String = math.min(x.length, y.length).toString
      println(s"x: ${x}, y: ${y}\tx and y min length is ${s}")
      s
    },
    _ + _)
  //x: , y: 12	x and y min length is 0
  //x: , y: 345	x and y min length is 0
  //x: 0, y: 23	x and y min length is 1
  //x: 0, y: 1	x and y min length is 1
  println(str3) //11

}
