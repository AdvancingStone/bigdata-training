package com.bluehonour.transformation

import com.bluehonour.SparkContext
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD

/**
 * Repartition the RDD according to the given partitioner and within each resulting partition, sort records by their keys.
 */
object RepartitionAndSortWithinPartitions extends SparkContext with App {

  // first we will do range partitioning which is not sorted
  val randRDD = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
  val rPartitioner: RangePartitioner[Int, String] = new RangePartitioner(3, randRDD)
  val partitioned: RDD[(Int, String)] = randRDD.partitionBy(rPartitioner)

  def myfunc(index: Int, iter: Iterator[(Int, String)]): Iterator[String] = {
    iter.map(x => "[partID:" + index + ", val: " + x + "]")
  }

  val result: Array[String] = partitioned.mapPartitionsWithIndex(myfunc).collect
  println(result.mkString(","))
  //  [partID:0, val: (2,cat)],[partID:0, val: (3,book)],[partID:0, val: (1,screen)],
  //  [partID:1, val: (4,tv)],[partID:1, val: (5,heater)],
  //  [partID:2, val: (6,mouse)],[partID:2, val: (7,cup)]

  // now lets repartition but this time have it sorted
  val partitioned2 = randRDD.repartitionAndSortWithinPartitions(rPartitioner)
  val result2: Array[String] = partitioned2.mapPartitionsWithIndex(myfunc).collect
  println(result2.mkString(","))
  //  [partID:0, val: (1,screen)],[partID:0, val: (2,cat)],[partID:0, val: (3,book)],
  //  [partID:1, val: (4,tv)],[partID:1, val: (5,heater)],
  //  [partID:2, val: (6,mouse)],[partID:2, val: (7,cup)]

}
