package com.bluehonour.transformation

object FlatMap extends SparkContext {
  def main(args: Array[String]): Unit = {

    val value = sc.parallelize(1 to 10, 5)
      .flatMap(1 to _).collect
    println(value.mkString(","))

    val value2 = sc.parallelize(List(1, 2, 3), 2)
      .flatMap(x => {
        List(x, x, x)
      }).collect
    println(value2.mkString(","))

    val x = sc.parallelize(1 to 10, 3)
    x.flatMap(List.fill(2)(_)).collect.foreach(print)
  }

}
