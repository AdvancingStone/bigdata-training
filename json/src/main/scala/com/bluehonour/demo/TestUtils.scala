package com.bluehonour.demo

import java.io.{File, FileOutputStream}

import com.bluehonour.demo.utils.SftpClientUtil
import com.jcraft.jsch.Session
import org.junit.{After, Before, Test}

class TestUtils {
  var session: Session = _

  val inputFile = "D:\\home\\xxx\\tmp\\config.txt"
    val ip = "master02"
    val user = "root"
    val pwd = "redhat"
    val port = 22
    val path = "/home/tmp/wawa"
    val file = path + "/config.csv"
    val outputFile = "D:\\home\\xxx\\tmp\\temp.txt";

  @Before
  def setup(): Unit = {
    session = SftpClientUtil.getSftpSession(ip, user, pwd, port)
  }

  @After
  def after(): Unit = {
    session.disconnect()
  }

  @Test
  def testIsExistsDoneFilet():Unit={
    val bool = SftpClientUtil.isExistsDoneFile(ip, user, pwd, port, "/20210108/20210108.done")
    println(bool)
  }

  @Test
  def testSshSftpUpLoadFile = {
    val file = new File(inputFile)
    val bool = SftpClientUtil.sshSftpUpLoadFile(ip, user, pwd, port, path, file)
    println(bool)
  }

  @Test
  def testSshSftpDownLoadFile = {
    val bool = SftpClientUtil.sshSftpDownLoadFile(ip, user, pwd, port, file, outputFile)
    println(bool)
  }

  @Test
  def testReadFileAndDownload: Unit = {
    val stream = SftpClientUtil.readFile(session, file)
    val fos = new FileOutputStream(outputFile)
    val buffer = new Array[Byte](1024)
    var size: Int = 0
    while (size != -1) {
      fos.write(buffer, 0, size)
      size = stream.read(buffer)
    }
    stream.close()
    fos.close()
    println("succeed")
  }

}
