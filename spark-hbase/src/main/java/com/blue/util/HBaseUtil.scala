package com.blue.util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.io.InputStream
import java.util


object HBaseUtil {


  def main(args: Array[String]): Unit = {
        println(getHBaseConn.toString)
    //    val table: Table = getTable("tbl_user")
    //    //    deleteAllColumnName(table, "info", "name")
    //
    //    deleteRangeRowkey(table, "rk3", "rk4")
    //    closeAll(getHBaseConn, table)
    //    println(isExistsRowkey(getConn, "cc:purchaseSendCouponPackage", "1"))
    //    println(isExistsRowkey(getConn, "cc:purchaseSendCouponPackage", "2"))
    //    println(isExistsRowkey(getConn, "cc:purchaseSendCouponPackage", "11"))
    //    println(isExistsRowkey(getConn, "cc:purchaseSendCouponPackage", "4"))
    //    println(isExistsRowkey(getConn, "cc:purchaseSendCouponPackage", "44"))
  }

  private val hbase = new HBaseUtil

  val getHBaseConf: Configuration = {
     hbase.getLocalHBaseConf
  }

  val getHBaseConn: Connection = {
     hbase.getlocalConn
  }

  /**
    * 获得操作类Table
    *
    * @param tableName
    * @return
    */
  def getTable(tableName: String): Table = {
    var table: Table = null
    try {
      table = getHBaseConn.getTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception => println(">>>获取Table对象失败:" + e)
    }
    table
  }


  /**
    * 根据rowKey精确查询单行数据单行值
    *
    * @param rowkey
    * @param family
    * @param column
    * @return
    */
  def getByRowkey(table: Table, rowkey: String, family: String, column: String): String = {
    var value: String = null
    try {
      //单个get查询
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
      val result = table.get(get)
      value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)))
    } catch {
      case e: Exception => println(">>>查询失败:" + e)
    }
    value
  }

  /**
    * 判断rowkey存不存在
    *
    * @param conn
    * @param tableName
    * @param rowkey
    * @return
    */
  def isExistsRowkey(conn: Connection, tableName: String, rowkey: String): Boolean = {
    if (rowkey == null) {
      return false
    }
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(rowkey.getBytes)
    val result: Result = table.get(get)
    !result.isEmpty
  }

  /**
    * 添加一行数据
    *
    * @param table  操作表的对象
    * @param rowKey key
    * @param family 簇
    * @param column 列
    * @param value  需要添加的数据内容
    */
  def addRow(table: Table, rowKey: String, family: String, column: String, value: String): Unit = {
    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
    rowPut.addColumn(family.getBytes, column.getBytes, value.getBytes)
    table.put(rowPut)
  }

  /**
    * 删除所有的列名
    *
    * @param tableName
    * @param columnFamily
    * @param columnName
    */
  def deleteAllColumnName(table: Table, columnFamily: String, columnName: String): Unit = {
    val scan: Scan = new Scan()
    scan.addColumn(columnFamily.getBytes(), columnName.getBytes())
    val iterator: util.Iterator[Result] = table.getScanner(scan).iterator()
    val deleteList = new util.ArrayList[Delete](100000)
    var count = 0;
    while (iterator.hasNext) {
      val result: Result = iterator.next()
      val delete = new Delete(result.getRow)
      if (result.containsColumn(columnFamily.getBytes(), columnName.getBytes())) {
        delete.addColumns(columnFamily.getBytes(), columnName.getBytes())
        deleteList.add(delete)
        count = count + 1;
      }
      if (deleteList.size() == 100000) {
        table.delete(deleteList)
        deleteList.clear()
        println(s"已经删除${count}条记录")
      }
    }
    if (deleteList.size() > 0) {
      table.delete(deleteList)
      deleteList.clear()
      println(s"已经删除${count}条记录")
    }
  }

  /**
    * 删除指定范围的所有rowkey， 前闭后开 [startRow, endRow)
    *
    * @param table
    * @param startRow 开始行
    * @param endRow   结束行
    */
  def deleteRangeRowkey(table: Table, startRow: String, endRow: String): Unit = {
    val scan: Scan = new Scan()
    scan.withStartRow(startRow.getBytes()).withStopRow(endRow.getBytes())
    val iterator: util.Iterator[Result] = table.getScanner(scan).iterator()
    val deleteList = new util.ArrayList[Delete](100000)
    var count = 0;
    while (iterator.hasNext) {
      val result: Result = iterator.next()
      val delete = new Delete(result.getRow)
      deleteList.add(delete)
      count = count + 1;
      if (deleteList.size() == 100000) {
        table.delete(deleteList)
        deleteList.clear()
        println(s"已经删除${count}条记录")
      }
    }
    if (deleteList.size() > 0) {
      table.delete(deleteList)
      deleteList.clear()
      println(s"已经删除${count}条记录")
    }
  }

  /**
    * 关闭所有连接
    *
    * @param conn
    * @param tables
    */
  def closeAll(conn: Connection, tables: Table*): Unit = {
    try {
      tables.foreach(stmt => {
        if (stmt != null) {
          stmt.close()
        }
      })
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => {
        println(s"$e")
      }
    }
  }

}

class HBaseUtil private {



  def getLocalHBaseConf:Configuration = {
    val config = HBaseConfiguration.create
    val stream1: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/core-site.xml")
    val stream2: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/hbase-site.xml")
    val stream3: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/hdfs-site.xml")
    config.addResource(stream1)
    config.addResource(stream2)
    config.addResource(stream3)
    config.set(TableInputFormat.INPUT_TABLE, "u_analysis:purchase_send_coupon_new6_hbase")
    config
  }



  def getlocalConn: Connection = {
    val conn: Connection = ConnectionFactory.createConnection(getLocalHBaseConf)
    println(s"HBase Connection: ${conn} is already connected ${conn}")
    conn
  }

}

