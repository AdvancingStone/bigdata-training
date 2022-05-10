package com.blue.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinOptions {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (1L, ("a", "student")), (2L, ("b", "salesman")),
        (3L, ("c", "programmer")), (4L, ("d", "doctor")),
        (5L, ("e", "postman"))
      ))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, "customer"), Edge(3L, 2L, "customer"),
        Edge(3L, 4L, "patient"), Edge(5L, 4L, "patient"),
        Edge(3L, 4L, "friend"), Edge(5L, 99L, "father")))

    val defaultUser = ("f", "none")

    val graph = Graph(users, relationships, defaultUser)

    val userWithAge: RDD[(VertexId, Int)] =
      sc.parallelize(Array(
        (3L, 2), (4L, 19), (5L, 23), (6L, 42), (7L, 59)
      ))

    //outerJoinVertices
    graph.outerJoinVertices(userWithAge){
      (id, attr, age) => {
        age match {
          case Some(a) => (attr._1, attr._2, a)
          case None => (attr._1, attr._2)
        }
      }
    }.vertices.collect.foreach(println)

    graph.joinVertices(userWithAge){
      (id, attr, age) => {
        (attr._1 + "", attr._2 + "、" + age)
      }
    }.vertices.collect.foreach(println)

    sc.stop()

  }
}
