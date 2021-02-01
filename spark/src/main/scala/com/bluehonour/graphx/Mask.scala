package com.bluehonour.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 取两个图的公共顶点和边作为新图，并保持前一个图顶点与边的属性
 */
object Mask {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")
      ))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //Run Connected Components
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents() //No longer contains missing field
    ccGraph.vertices.collect.foreach(println(_))
    ccGraph.edges.collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    val validCCGraph: Graph[VertexId, String] = ccGraph.mask(validGraph)
    validCCGraph.vertices.collect.foreach(println(_))
    validCCGraph.edges.collect.foreach(println(_))

    validCCGraph.reverse.edges.collect.foreach(println(_))

  }


}
