package com.blue.transformation

import com.blue.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Works like the aggregate function except the aggregation is applied to the values with the same key.
 * Also unlike the aggregate function the initial value is not applied to the second reduce.
 *
 */
object AggregateByKey extends SparkContext with App {

  val pairRDD: RDD[(String, Int)] = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)

  // lets have a look at what is in the partitions
  def myfunc(index: Int, iterator: Iterator[(String, Int)]): Iterator[String] = {
    iterator.map(x => s"[partID: ${index}, val: $x]")
  }

  val collectArr: Array[String] = pairRDD.mapPartitionsWithIndex(myfunc).collect
  println(collectArr.mkString(","))
  //  [partID: 0, val: (cat,2)],[partID: 0, val: (cat,5)],[partID: 0, val: (mouse,4)],[partID: 1, val: (cat,12)],[partID: 1, val: (dog,12)],[partID: 1, val: (mouse,2)]

  val result1: Array[(String, Int)] = pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect()
  println(result1.mkString(","))
  // (dog,12),(cat,17),(mouse,6)

  val result2: Array[(String, Int)] = pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
  println(result2.mkString(","))
  // (dog,100),(cat,200),(mouse,200)

}
