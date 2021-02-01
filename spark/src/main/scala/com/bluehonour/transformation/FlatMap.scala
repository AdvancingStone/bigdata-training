package com.bluehonour.transformation

import com.bluehonour.SparkContext

object FlatMap extends SparkContext with App {
  val value = sc.parallelize(1 to 10, 5)
    .flatMap(1 to _).collect
  println(value.mkString(","))
  //1,1,2,1,2,3,1,2,3,4,1,2,3,4,5,1,2,3,4,5,6,1,2,3,4,5,6,7,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,10

  val value2 = sc.parallelize(List(1, 2, 3), 2)
    .flatMap(x => {
      List(x, x, x)
    }).collect
  println(value2.mkString(","))
  //1,1,1,2,2,2,3,3,3

  val x = sc.parallelize(1 to 10, 3)
  x.flatMap(List.fill(2)(_)).collect.foreach(print)
  //1122334455667788991010
}
