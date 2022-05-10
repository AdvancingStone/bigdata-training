package com.blue.demo

import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkSession: sql.SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: spark.SparkContext = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val rdd: RDD[String] = sc.parallelize(
      List(
        "memberid1,1",
        "memberid2,2",
        "memberid3,3",
        "memberid4,4",
        "memberid5,5")
      ,1
    )

    val rdd2: RDD[String] = rdd.map(x => {
      val split = x.split(",")
      val msrId = split(0)
      val expireValue = split(1)
      val res = s"""{"msrId":"${msrId}","expireValue":${expireValue},"date":${System.currentTimeMillis()}}"""
//      println(res)
      res
    })


    rdd2.foreachPartition(it => {
      val list = new java.util.ArrayList[String](2)
      it.foreach(item => {
        list.add(item)
        if (list.size() >= 2) {

          //          val httpClient = SkipHttpsUtil.wrapClient()
          val httpPut = new HttpPut("url")
          httpPut.addHeader("Content-Type", "application/json")
          httpPut.setHeader("apiKey", "apiKey")
          println("list", list.toArray().mkString(","))
          val entity: StringEntity = new StringEntity("[" + list.toArray().mkString(",") + "]", "UTF-8")
          println("entity", entity)
          httpPut.setEntity(entity)
          //          val responseBody = httpClient.execute(httpPut)
          //          val responseCode = responseBody.getStatusLine.getStatusCode
          //          println(url + ":" + item + ":" + responseCode)
          list.clear()
        }
      })
      if (list.size() > 0) {
        println("222222222222")
        //          val httpClient = SkipHttpsUtil.wrapClient()
        val httpPut = new HttpPut("url")
        httpPut.addHeader("Content-Type", "application/json")
        httpPut.setHeader("apiKey", "apiKey")
        val entity: StringEntity = new StringEntity("[" + list.toArray().mkString(",") + "]", "UTF-8")
        println("entity", entity)
        httpPut.setEntity(entity)
        //          val responseBody = httpClient.execute(httpPut)
        //          val responseCode = responseBody.getStatusLine.getStatusCode
        //          println(url + ":" + item + ":" + responseCode)
        list.clear()
      }
    })
    //    HttpClientUtil.callRuleEngine()
  }
}
