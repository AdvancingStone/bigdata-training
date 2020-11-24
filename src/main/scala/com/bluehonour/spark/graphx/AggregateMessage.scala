package com.bluehonour.spark.graphx

import org.apache.spark.graphx.{EdgeContext, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object AggregateMessage {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getCanonicalName}").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 5).mapVertices((id, _) => id.toDouble)
    graph.triplets.collect.foreach(e => println(s"${e.srcId}, ${e.dstId}, ${e.srcAttr}, ${e.dstAttr}, ${e.attr}"))

    // Compute the number of older followers and their total age
    val oldFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => {  // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2)// Reduce Function
    )

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] = oldFollowers.mapValues((id, value) => {
      value match {
        case (count, totalAge) => totalAge / count
      }
    })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println)

    def msgFun(triplet: EdgeContext[Double, Int, String]): Unit = {
      triplet.sendToDst("Hi")
    }
    def reduceFun(a:String, b:String):String = a +" " + b
    val result = graph.aggregateMessages[String](msgFun, reduceFun)
    result.collect.foreach(println)

    val v1: VertexRDD[Array[(VertexId, Double)]] = graph.collectNeighbors(EdgeDirection.In)
    v1.collect.foreach(v => {
      v._2.foreach(v2 => println(s"${v._1}, ${v2.toString()}"))
    })

    val v2: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.In)
    v2.collect.foreach(v => {
      v._2.foreach(v2 => println(s"${v._1}, ${v2.toString()}"))
    })
  }

}
