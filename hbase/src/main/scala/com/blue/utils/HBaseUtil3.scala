package com.blue.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

import java.io.InputStream


class HBaseUtil3 private {
  def getProdConn: Connection = {
    val config = HBaseConfiguration.create
    val stream1: InputStream = this.getClass.getClassLoader.getResourceAsStream("hbase/core-site.xml")
    val stream2: InputStream = this.getClass.getClassLoader.getResourceAsStream("hbase/hbase-site.xml")
    val stream3: InputStream = this.getClass.getClassLoader.getResourceAsStream("hbase/hdfs-site.xml")
    config.addResource(stream1)
    config.addResource(stream2)
    config.addResource(stream3)

    System.setProperty("java.security.krb5.conf", "HBaseConfig.krb5")
    UserGroupInformation.setConfiguration(config)

    try {
      UserGroupInformation.setConfiguration(config)
      UserGroupInformation.loginUserFromKeytab("HBaseConfig.principal", "HBaseConfig.keytab")
    } catch {
      case e: Exception => println(s"kerberos认证失败: ${e.toString}")
    }

    val conn: Connection = ConnectionFactory.createConnection(config)
    println(s"HBase Connection: ${conn} is already connected ${conn}")
    conn
  }

  def getlocalConn: Connection = {
    val config = HBaseConfiguration.create
    val stream1: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/core-site.xml")
    val stream2: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/hbase-site.xml")
    val stream3: InputStream = this.getClass.getClassLoader.getResourceAsStream("local_hbase/hdfs-site.xml")
    config.addResource(stream1)
    config.addResource(stream2)
    config.addResource(stream3)
    val conn: Connection = ConnectionFactory.createConnection(config)
    println(s"HBase Connection: ${conn} is already connected ${conn}")
    conn
  }

}

