package com.bluehonour.spark.graphx

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test01 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
      .master("local[*]")
      //      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    // Create an RDD for the vertices
    val vertexArray: Array[(VertexId, (String, String))] = Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "professor")),
      (2L, ("istoica", "professor"))
    )
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(vertexArray)

    // Create an RDD for edges
    val edgeArray: Array[Edge[String]] = Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")
    )
    val relationships: RDD[Edge[String]] = sc.parallelize(edgeArray)

    //Define a default user in case there are ralationship with missing user
    val defaultUser = ("John Doe", "Missing")
    //Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)

    //foreach all users which are postdoc
    graph.vertices.filter{
      case (id, (name, pos)) => pos=="postdoc"
    }.collect().foreach(e=> println(s"${e._1}, ${e._2}"))

    //Count all users which are professor
    val count: VertexId = graph.vertices.filter {
      case (id, (name, pos)) => pos == "professor"
    }.count
    println(count)

    //foreach all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).collect().foreach(e => println(s"${e.srcId}, ${e.dstId}, ${e.attr}"))
    // Count all the edges where src > dst
    val count2: VertexId = graph.edges.filter(e => e.srcId > e.dstId).count()
    println(count2)

    //Use the triplets view to create an RDD of facts.
    val triplets: RDD[EdgeTriplet[(String, String), String]] = graph.triplets
    triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    //Use the implicit GraphOps.inDegrees operator
    graph.inDegrees.collect.foreach(v => println(s"${v._1} has ${v._2} inDegrees"))
    graph.outDegrees.collect.foreach(v => println(s"${v._1} has ${v._2} outDegrees"))
    graph.degrees.collect.foreach(v => println(s"${v._1} has ${v._2} degrees"))

    //Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int, String] = graph.outerJoinVertices(graph.outDegrees)((vid, vd, degOpt) => {
      println(s"vid: ${vid}, vd ${vd}, degOpt: ${degOpt}")
      degOpt.getOrElse(0)
    })
    inputGraph.triplets.collect.foreach(x => println(s"${x.toString()}"))
    println()
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    val outputGraph: Graph[Double, Double] = inputGraph.mapTriplets(triplet => {
      println(s"srcAttr: ${triplet.srcAttr}, toString: ${triplet.toString()}")
      println(s"srcAttr:${triplet.srcAttr}, dstAttr:${triplet.dstAttr}, srcId:${triplet.srcId}, dstId:${triplet.dstId}, attr:${triplet.attr}")
      1.0 / triplet.srcAttr
    })
      .mapVertices((id, outerDegree) => {
        println(s"id: ${id}, outerDegree: ${outerDegree}")
        1.0
      })
    outputGraph.triplets.collect.foreach(x => println(s"triplets ${x.srcId}, ${x.dstId}, ${x.srcAttr}, ${x.dstAttr}, ${x.attr}"))
    outputGraph.vertices.collect.foreach(x => println(s"vertices ${x._1}, ${x._2}, ${x.toString()}"))




    sc.stop()
  }


}
