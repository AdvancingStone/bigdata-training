package com.blue.graphx

import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object VertexRDD2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val setA: VertexRDD[Double] = VertexRDD(sc.parallelize(0L until 10L).map(id => (id, 1.0)))
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(5 until 15).flatMap(id => List((id, 1.0), (id, 2.0)))
    println(rddB.count()) //20
    rddB.collect.foreach(e => println("rddB", e._1, e._2))

    println("********* aggregateUsingIndex **************")
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    println(setB.count()) //5
    setB.collect.foreach(e => println("setB",e._1, e._2))

    //Joining A and B should now be fast!
    println("************  inner join  ******************")
    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
    setC.collect.foreach(println)

    println("*************  left join  *******************")
    val setD= setA.leftJoin(setB)((id, a, b) => {
      a.toDouble + b.getOrElse(0.0)
    })
    setD.collect.foreach(println)

    println("****************  filter   *****************")
    setA.filter(a => a._1 > 5).collect.foreach(println)

    println("**************     mapValues    **************")
    setA.mapValues(a => a*2).collect.foreach(println)

    println("************       minus   ***************")
    setA.minus(setB).collect.foreach(println)
    setA.minus(rddB).collect.foreach(println)

    println("************   diff   *********************")
    setA.diff(setB).collect.foreach(println)
    setB.diff(rddB).collect.foreach(println)

  }

}
