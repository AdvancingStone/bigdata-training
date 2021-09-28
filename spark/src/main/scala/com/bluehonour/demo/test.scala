package com.bluehonour.demo

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[1]")
      //      .config("hive.metastore.uris", "thrift://master:9083")
      //      .enableHiveSupport()
      .getOrCreate()
    val sc: spark.SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    val list: immutable.Seq[(String, (String, String, Int))] = List(
      ("key1", ("order1", "20210909", -12)),
      ("key2", ("order2", "20210908", -32)),
      ("key3", ("order3", "20210914", -22))
    )

    val list2: immutable.Seq[(String, (String, String, Int))] = List(
      ("key1", ("order4", "20210903", 13)),
      ("key2", ("order5", "20210903", 43)),
      ("key3", ("order6", "20210913", 5))
    )
    val value1: RDD[(String, (String, String, Int))] = sc.parallelize(list)
    val value2: RDD[(String, (String, String, Int))] = sc.parallelize(list2)

//    value1.foreach(data => {
//      println(data)
//    })
//    value2.foreach(data => {
//      println(data)
//    })

    val result: RDD[String] = value1.union(value2).groupByKey().flatMap(data => {
      println(1, data)
      val pool: ArrayBuffer[String] = new ArrayBuffer[String]()
      val invalidOrderPool: ArrayBuffer[String] = new ArrayBuffer[String]()
      data._2.toArray.sortBy(_._2).reverse.foreach(data => {
        println("\t",2, data)
        println(data._3)
        if (data._3 < 0) {
          pool += data._1
        } else {
          if (pool.nonEmpty) {
            invalidOrderPool += data._1
            println(s"data._1  ${data._1}")
            invalidOrderPool += pool.head
            println(s"pool.head ${pool.head}")
            println("size1",pool.size)
            pool.trimStart(1)
            println("size2",pool.size)
          }
        }
      })
      invalidOrderPool
    })

    result.foreach(data=>{
      println(s"result: ${data}")
    })

  }

}
