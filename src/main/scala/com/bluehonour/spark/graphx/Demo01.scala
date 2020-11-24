package com.bluehonour.spark.graphx
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    //设置顶点和边，注意顶点和边都是用元组定义的 Array
    //顶点的数据类型是 VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    //边的数据类型 ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(VertexId, (String, PartitionID))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[PartitionID]] = sc.parallelize(edgeArray)
    //构造图
    val graph: Graph[(String, PartitionID), PartitionID] = Graph(vertexRDD, edgeRDD)

    println("**********************************************************")
    println("                      属性演示")
    println("**********************************************************")
    println("找出图中年龄大于 30 的顶点：")

    graph.vertices.filter {
      case (id, (name, age)) => age > 30
    }.collect.foreach {
      case (id, (name, age)) => println(s"${name} is ${age}")
    }
    println("**********************************************************")
    graph.triplets.foreach(t=>println(s"triplet:${t.srcId}, ${t.srcAttr}, ${t.dstId}, ${t.dstAttr}, ${t.attr}, ${t.toString()}"))
    println("**********************************************************")
    //边操作，找出图中大于5的边
    println("                  找出图中大于5的边")
    graph.edges.filter(e=>e.attr>5).collect().foreach(e=>println(s"${e.srcId}, ${e.dstId}, ${e.attr}"))
    println("**********************************************************")
    //triplets 操作，((srcId, srcAttr),(dstId, dstAttr), attr)
    println("              列出边属性>5的 tripltes: ")
    for(triplet <- graph.triplets.filter(t=>t.attr>5).collect()){
      println(s"${triplet.srcAttr._1} -> ${triplet.dstAttr._1}")
    }

    graph.triplets.filter(t=>t.attr>5).collect.foreach(triplet => println(s"${triplet.srcAttr._1} -> ${triplet.dstAttr._1}"))

    //Degrees操作
    println("**********************************************************")
    println("           Degrees操作")
    println("**********************************************************")
    println("           找出图中最大的出度、入度、度数： ")
    println("max of outDegrees: " + graph.outDegrees.reduce(max))
    println("max of inDegrees: " + graph.inDegrees.reduce(max))
    println("max if Degrees: " + graph.degrees.reduce(max))

    println("**********************************************************")
    println("                      转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点 age + 10：")
    graph.mapVertices{
      case (id, (name, age)) => (id, (name, age+10))
    } .vertices.collect()
      .foreach(v => println(s"${v._2._1}'age add 10 is ${v._2._2}"))

    println("边的转换操作，边的属性 *2：")
    graph.mapEdges(e => e.attr*2)
        .edges.collect()
        .foreach(e => println(s"${e.srcId} to ${e.dstId}, attL: ${e.attr}"))

    println("**********************************************************")
    println("                   结构操作")
    println("**********************************************************")
    println("顶点年纪>30 的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 > 30)
    println("子图所有顶点：")
    subGraph.vertices.collect().foreach(v => println(s"${v._1}:  ${v._2._1} is ${v._2._2}"))
    println("子图所有的边：")
    subGraph.edges.collect().foreach(e => println(s"${e.srcId}->${e.dstId} attr is ${e.attr}"))

    println("**********************************************************")
    println("连接操作")
    println("**********************************************************")
    val inDegrees: VertexRDD[PartitionID] = graph.inDegrees
    //创建一个新图，顶点 VD 的数据类型为 User, 并从 graph 做类型转换
    val initialUserGraph: Graph[User, PartitionID] = graph.mapVertices {
      case (id, (name, age)) => User(name, age, 0, 0)
    }
    //initialUserGraph 与 inDegrees、outDegrees（RDD）进行连接，并修改 initialUserGraph中 inDeg 值、outDeg 值
    val userGraph: Graph[User, PartitionID] = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, user, inDegOpt) => User(user.name, user.age, inDegOpt.getOrElse(0), user.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, user, outDegOpt) => User(user.name, user.age, user.inDeg, outDegOpt.getOrElse(0))
    }
    println("连接图的属性")
    userGraph.vertices.collect.foreach(v => println(s"id: ${v._1}: ${v._2.name} inDeg: ${v._2.inDeg}, outDeg: ${v._2.outDeg}"))

    println("出度和入度相同的人员： ")
    userGraph.vertices.filter{
      case (id, user) => user.inDeg == user.outDeg
    }.collect.foreach{
      case(id, property) => println(id, property.name)
    }

    println("**********************************************************")
    println("                   聚合操作")
    println("**********************************************************")
    println("               找出年纪最大的follower：")
    val oldestFollower: VertexRDD[(String, PartitionID)] = userGraph.aggregateMessages[(String, PartitionID)](
      //将原顶点的属性发送给目标顶点，map过程
      et => et.sendToDst((et.srcAttr.name, et.srcAttr.age)),
      //得到最大follower， reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )

    oldestFollower.collect.foreach(o => println(o.toString()))

    userGraph.vertices.leftJoin(oldestFollower){
      (id, user, optOldestFollower) =>
        optOldestFollower match{
          case None => s"${user.name} does not have any followers."
          case Some((name, age)) => s"${name} is the oldest follower of ${user.name}"
        }
    }.collect.foreach{ case (id, str) => println(str)}

    println("**********************************************************")
    println("                      聚合操作")
    println("**********************************************************")
    println("               找出距离最远的顶点，Pregel基于对象")
//    val g: Graph[(PartitionID, VertexId), PartitionID] = Pregel(graph.mapVertices((vid, vd) => (0, vid)), (0, Long.MinValue), activeDirection = EdgeDirection.Out)(
//      (id: VertexId, vd: (PartitionID, VertexId), a: (PartitionID, VertexId)) => math.max(vd._1, a._1) match {
//        case vd._1 => vd
//        case a._1 => a
//      },
//      (et: EdgeTriplet[(PartitionID, VertexId), PartitionID]) => Iterator((et.dstId, (et.srcAttr._1 + 1 + et.attr, et.srcAttr._2))),
//      (a: (PartitionID, VertexId), b: (PartitionID, VertexId)) => math.max(a._1, b._1) match {
//        case a._1 => a
//        case b._1 => b
//      }
//    )
//    g.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))

    // 面向对象
    val g2: Graph[(PartitionID, VertexId), PartitionID] = graph.mapVertices((vid, vd) => (0, vid)).pregel((0, Long.MinValue), activeDirection = EdgeDirection.Out)(
      (id: VertexId, vd: (PartitionID, VertexId), a: (PartitionID, VertexId)) => math.max(vd._1, a._1) match {
        case vd._1 => vd
        case a._1 => a
      },
      (et: EdgeTriplet[(PartitionID, VertexId), PartitionID]) => Iterator((et.dstId, (et.srcAttr._1 + 1, et.srcAttr._2))),
      (a: (PartitionID, VertexId), b: (PartitionID, VertexId)) => math.max(a._1, b._1) match {
        case a._1 => a
        case b._1 => b
      }
    )
    println()
    g2.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))
    sc.stop()
  }

  case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
  def max(a: (VertexId, Int), b: (VertexId, Int)) : (VertexId, Int) = {
    if(a._2 > b._2) a else b
  }
}
