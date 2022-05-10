package com.blue.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test02 {

  def main(args: Array[String]): Unit = {
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //设置users顶点
    val users: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Array(
        (1L, ("Alice", 28)),
        (2L, ("Bob", 27)),
        (3L, ("Charlie", 65)),
        (4L, ("David", 42)),
        (5L, ("Ed", 55)),
        (6L, ("Fran", 50))
      ))

    //设置relationships边
    val relationships: RDD[Edge[Int]] =
      sc.parallelize(Array(
        Edge(2L, 4L, 7),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(5L, 6L, 3)
      ))
    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser = ("John Doe", 0)

    println("（1）通过上面的项点数据和边数据创建图对象")
    // Build the initial Graph
    val graph: Graph[(String, Int), Int] = Graph(users, relationships,
      defaultUser)
    graph.edges.collect.foreach(println(_))
    graph.vertices.collect.foreach(println(_))


    //设置users顶点
    val users2: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Array(
        (1L, ("Alice2", 28)),
        (2L, ("Bob2", 27)),
        (3L, ("Charlie2", 65)),
        (4L, ("David2", 42))
      ))

    //设置relationships边
    val relationships2: RDD[Edge[Int]] =
      sc.parallelize(Array(
        Edge(2L, 3L, 7),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(4L, 1L, 1)
      ))
    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser2 = ("Missing", 0)

    println("（1.1）通过上面的项点数据和边数据创建图对象")
    // Build the initial Graph
    val graph2: Graph[(String, Int), Int] = Graph(users2, relationships2,
      defaultUser2)
    graph2.edges.collect.foreach(println(_))
    graph2.vertices.collect.foreach(println(_))
    println("（2）对上述的顶点数据和边数据进行修改，再创建一张图2，使得图2中有一些点和边在图1中不存在，然后调用图1的mask方法，传入图2作为参数，观察返回的图3的信息")


    println("mask效果：返回公共子图")
    val graph3 = graph.mask(graph2)
    graph3.edges.collect.foreach(println(_))
    graph3.vertices.collect.foreach(println(_))

    println("（3）基于上面的数据进行修改练习subgraph 、groupEdges 、reverses方法")

    println("-----------subgraph")
    var graph4: Graph[(String, Int), Int] = graph.subgraph(epred = (ed) => !(ed.srcId == 1L || ed.dstId == 2L)
      , vpred = (id, attr) => id != 3L)
    graph4.edges.collect.foreach(println(_))
    graph4.vertices.collect.foreach(println(_))

    println("-----------groupEdges")
    var graph5: Graph[(String, Int), Int] = graph.groupEdges(merge = (ed1,
                                                                      ed2) =>
      (ed1 + ed2 + 100000000))
    graph5.edges.collect.foreach(println(_))
    //    graph5.vertices.collect.foreach(println(_))


    println("----------reverses")
    graph2.edges.foreach(println(_))
    println("-------")
    graph2.reverse.edges.foreach(println(_))


  }


}