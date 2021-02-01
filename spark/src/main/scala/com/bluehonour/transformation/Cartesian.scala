package com.bluehonour.transformation

/**
 * Computes the cartesian product between two RDDs
 * (i.e. Each item of the first RDD is joined with each item of the second RDD) and returns them as a new RDD.
 * (Warning: Be careful when using this function.! Memory consumption can quickly become an issue!)
 */
object Cartesian extends SparkContext with App {
  val x = sc.parallelize(List(1, 2, 3, 4, 5))
  val y = sc.parallelize(List(6, 7, 8, 9, 10))
  val res: Array[(Int, Int)] = x.cartesian(y).collect
  println(res.mkString(","))
  //  (1,6),(1,7),(1,8),(1,9),(1,10),(2,6),(2,7),(2,8),(2,9),(2,10),(3,6),(3,7),(3,8),(3,9),(3,10),(4,6),(4,7),(4,8),(4,9),(4,10),(5,6),(5,7),(5,8),(5,9),(5,10)
}
