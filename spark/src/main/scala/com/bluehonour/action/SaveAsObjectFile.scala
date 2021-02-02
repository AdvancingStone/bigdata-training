package com.bluehonour.action

import com.bluehonour.SparkContext

/**
 * Saves the RDD in binary format.
 */
object SaveAsObjectFile extends SparkContext with App {

  val x = sc.parallelize(1 to 10, 3)
  x.saveAsObjectFile("objFile")
  val y: Array[Int] = sc.objectFile[Int]("objFile").collect()
  println(y.mkString(","))
  //  1,2,3,4,5,6,7,8,9,10
}
