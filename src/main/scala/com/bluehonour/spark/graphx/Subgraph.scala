package com.bluehonour.spark.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Subgraph {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
      .master("local[*]")
      //      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    // Remove missing vertices as well as the edges to connected to them
    println("\nRemove missing vertices")
    val validGraph: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    println("\nvalid edges equals 'colleague' and vertex equals 'prof'")
    //valid edges equals "pi"
    val valid2Graph: Graph[(String, String), String] = graph.subgraph(
      epred = relationships => relationships.attr == "colleague",
      vpred = (id, attr) => attr._2 == "prof"
    )
    valid2Graph.vertices.collect.foreach(println(_))
    valid2Graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    sc.stop()
  }

}
