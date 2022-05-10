package com.blue.graphx

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.sql.SparkSession

object Pregel {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    // A graph with edge attributes containing distances
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc,this.getClass().getClassLoader().getResource("web-Google.txt").getPath)
    graph.triplets.collect.foreach(e => println(s"${e.srcId}, ${e.dstId}, ${e.srcAttr}, ${e.dstAttr}, ${e.attr}"))
    // The ultimate source
    val sourceId: VertexId = 0
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph: Graph[Double, Int] = graph.mapVertices((id, _) => {
      if (id == sourceId) 0.0 else Double.PositiveInfinity //正无穷大
    })
    val sssp: Graph[Double, Int] = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), //vertex program
      triplet => { //send message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) //merge message
    )

    println(sssp.vertices.collect.mkString("\n"))

  }

}
