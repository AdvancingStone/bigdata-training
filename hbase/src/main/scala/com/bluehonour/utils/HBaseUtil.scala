package com.bluehonour.utils


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * hbase操作工具类
  */
object HBaseUtil {

  @transient lazy val logger:Logger = Logger.getLogger(this.getClass().getName())

  //获取配置文件的值
  //val hbaseParent:String = JavaUtil.getString("zookeeper.znode.parent")
  //val hbaseQuorum:String = JavaUtil.getString("hbase.zookeeper.quorum")
  //val hbaseClientPort:String = JavaUtil.getString("hbase.zookeeper.property.clientPort")
  //val tableName:String = JavaUtil.getString("hbase.table")

  val hbaseParent:String = "cnsz22pl0112,cnsz22pl0113,cnsz22pl0114,cnsz22pl0115"
  val hbaseQuorum:String = "2181"
  val hbaseClientPort:String = "/hbase"
  val tableName:String = "orderTable"


  /**
    * 获取hbase配置
    * @return
    */
  def getHbaseConf(): Configuration ={
    var hbaseConf: Configuration=null

    try{
      //获取hbase的conf
      hbaseConf = HBaseConfiguration.create()
      //设置写入的表
      hbaseConf.set("zookeeper.znode.parent", hbaseParent)
      hbaseConf.set("hbase.zookeeper.quorum", hbaseQuorum)
      hbaseConf.set("hbase.zookeeper.property.clientPort", hbaseClientPort)
    }catch{
      case e:Exception => logger.error(">>>连接hbase失败:,"+e)
    }

    hbaseConf
  }

  /**
    * 获得操作类Table
    * @param tableName
    * @return
    */
  def getTable(tableName:String): Table ={
    var table: Table =null
    try {
      val hbaseConf = getHbaseConf()
      val conn = ConnectionFactory.createConnection(hbaseConf)
      table = conn.getTable(TableName.valueOf(tableName))
    } catch {
      case e:Exception =>logger.error(">>>获取Table对象失败:"+e)
    }
    table
  }


  /**
    * 添加一行数据
    * @param table 操作表的对象
    * @param rowKey key
    * @param family 簇
    * @param column 列
    * @param value 需要添加的数据内容
    */
  def addRow(table:Table,rowKey:String,family:String,column:String,value:String): Unit ={
    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
    if (value == null) {
      rowPut.addColumn(family.getBytes, column.getBytes, "".getBytes)
    } else {
      rowPut.addColumn(family.getBytes, column.getBytes, value.getBytes)
    }
    table.put(rowPut)
  }

  /**
    * 根据rowKey精确查询单行数据单行值
    * @param rowkey
    * @param family
    * @param column
    * @return
    */
  def getByKey(table:Table,rowkey:String, family:String, column:String): String ={
    var value: String=null
    try {
      //单个get查询
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result = table.get(get)
      value =  Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(column)))
    } catch {
      case e:Exception =>logger.error(">>>查询失败:"+e)
    }
    value
  }

  /**
    * 根据rowKey精确查询单行数据的多列值
    * @param rowkey
    * @param family
    * @param columns
    * @return
    */
  def getByKey(table:Table,rowkey:String, family:String, columns:Array[String]): ArrayBuffer[String] ={
    var list: ArrayBuffer[String]= new ArrayBuffer[String]()
    try {
      val get = new Get(Bytes.toBytes(rowkey))
      get.addFamily(family.getBytes())
      val result = table.get(get)
      columns.foreach(column=>{
        val value = Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(column)))
        list +=  value
      })
    } catch {
      case e:Exception =>logger.error(">>>查询失败:"+e)
    }
    list
  }

  /**
    * 根据subKey模糊批量查询多行数据
    * @param subKey
    * @param family
    * @param column
    * @return
    */
  def scanBySubKey(table:Table,subKey:String,family:String,column:String): ArrayBuffer[String] ={
    val resultList = new ArrayBuffer[String]()
    val table = getTable(tableName)
    //批量模糊查询
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(subKey))
    val scan = new Scan()
    scan.setFilter(filter)
    val rs:ResultScanner  =  table.getScanner(scan)
    for(result <- rs){
      if(result.containsColumn(Bytes.toBytes(family),Bytes.toBytes(column))){
        val value = Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(column)))
        resultList += value
      }
    }
    resultList
  }

  /**
    * 根据rowKey删除整行
    * @param table
    * @param rowKey
    */
  def deleteByKey(table:Table, rowKey:String): Unit ={
    try {
      table.delete(new Delete(rowKey.getBytes()))
    } catch {
      case e:Exception =>println(">>>删除操作失败："+e)
    }

  }

  /**
    * 删除rowKey的某簇的某列
    * @param table
    * @param rowKey
    */
  def deleteByColumn(table:Table, rowKey:String,family:String,column:String): Unit ={
    try {
      val delete = new Delete(rowKey.getBytes())
      delete.addColumn(family.getBytes(),column.getBytes())
      table.delete(delete)
    } catch {
      case e:Exception =>println(">>>删除操作失败："+e)
    }

  }

  /**
    * 删除rowKey的某个family
    * @param table
    * @param rowKey
    */
  def deleteByFamily(table:Table, rowKey:String,family:String): Unit ={
    try {
      val delete = new Delete(rowKey.getBytes())
      delete.addFamily(family.getBytes())
      table.delete(delete)
    } catch {
      case e:Exception =>println(">>>删除操作失败："+e)
    }

  }


  /**
    * 根据rowKey模糊删除
    * @param table
    * @param subKey
    */
  def deleteByKeys(table:Table,subKey:String): Unit ={
    try {
      //批量模糊删除
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(subKey))
      val scan = new Scan()
      scan.setFilter(filter)
      val scanners:ResultScanner  =  table.getScanner(scan)
      val keyList = new ArrayBuffer[Delete]()
      var delete:Delete  = null
      //注意：scala和java的for循环有区别，需要引入转换  import scala.collection.JavaConversions._
      for(scanner <- scanners){
        val rowKey = new String(scanner.getRow)
        delete = new Delete(rowKey.getBytes)
        keyList += delete
      }
      table.delete(keyList)
    } catch {
      case e:Exception =>println(">>>删除操作失败："+e)
    }

  }


  def main(args: Array[String]): Unit = {
    testHbase
  }

  def testHbase(): Unit ={
    val table  = getTable(tableName)
    val family = "info"
    val column = "value"
    //    deleteByKey(table,"18_13905083065_037563776718_2018-10-16 11:33:14:452")
    val value = scanBySubKey(table,"2018-10-16",family,column)
    //    deleteByKeys(table,"13959774493_037765802064")
    //    println(value.mkString(","))
    deleteByColumn(table,"01_18950728102_037563109294_2018-10-16 11:33:15:394","info","name")

  }


}