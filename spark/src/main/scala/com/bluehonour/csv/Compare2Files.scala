package com.bluehonour.csv

import com.bluehonour.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object Compare2Files extends SparkSession with App {

  import sparkSession.implicits._

  val path = "file:///localPath"
  val file1 = s"$path/diff.txt"
  val file2 = s"$path/diff_details.txt"
  val sc: SparkContext = sparkSession.sparkContext
  val sql: SQLContext = sparkSession.sqlContext
  sc.setLogLevel("ERROR")
  val rdd1: RDD[String] = sc.textFile(file1)
  val rdd2: RDD[String] = sc.textFile(file2)
  val df1: DataFrame = rdd1.toDF("id")
  val df2: DataFrame = rdd2.map(_.split("\t")).map(line => {
    line(1)
  }).toDF("id")

  df1.createOrReplaceTempView("table1")
  df2.createOrReplaceTempView("table2")

  df1.show(false)
  df2.show(false)


  sql.sql(
    """
      |select a.id as id1, b.id as id2 from table1 a full join table2 b on a.id=b.id where a.id is null or b.id is null
      |""".stripMargin).show()

}
