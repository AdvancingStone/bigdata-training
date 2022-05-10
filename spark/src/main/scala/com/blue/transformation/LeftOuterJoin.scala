package com.blue.transformation

import com.blue.SparkContext

object LeftOuterJoin extends SparkContext with App {
  val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  val b = a.keyBy(_.length)
  val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  val d = c.keyBy(_.length)
  val result: Array[(Int, (String, Option[String]))] = b.leftOuterJoin(d).collect
  println(result.mkString(","))
  //  (6,(salmon,Some(salmon))),
  //  (6,(salmon,Some(rabbit))),
  //  (6,(salmon,Some(turkey))),
  //  (6,(salmon,Some(salmon))),
  //  (6,(salmon,Some(rabbit))),
  //  (6,(salmon,Some(turkey))),
  //  (3,(dog,Some(dog))),
  //  (3,(dog,Some(cat))),
  //  (3,(dog,Some(gnu))),
  //  (3,(dog,Some(bee))),
  //  (3,(rat,Some(dog))),
  //  (3,(rat,Some(cat))),
  //  (3,(rat,Some(gnu))),
  //  (3,(rat,Some(bee))),
  //  (8,(elephant,None))

}
