package com.blue.action

import com.blue.SparkContext
import org.apache.hadoop.io.compress.GzipCodec

/**
 * Saves the RDD as text files. One line at a time.
 */
object SaveAsTextFile extends SparkContext with App {

  val a = sc.parallelize(1 to 10000, 3)
  //  Example without compression
  a.saveAsTextFile("mydata_a")
  //  Example with compression
  a.saveAsTextFile("mydata_b", classOf[GzipCodec])

  //  Example writing into HDFS
//  val x = sc.parallelize(List(1,2,3,4,5,6,6,7,9,8,10,21), 3)
//  x.saveAsTextFile("hdfs://localhost:8020/user/cloudera/test");
//
//  val sp = sc.textFile("hdfs://localhost:8020/user/cloudera/sp_data")
//  sp.flatMap(_.split(" ")).saveAsTextFile("hdfs://localhost:8020/user/cloudera/sp_x")
}
