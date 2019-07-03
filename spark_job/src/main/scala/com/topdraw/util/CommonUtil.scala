package com.topdraw.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object CommonUtil {
  private val logger = LoggerFactory.getLogger(CommonUtil.getClass)
  val conf: Configuration = new Configuration()
  conf.set("fs.default", "hdfs://ns1")
  val fs = FileSystem.get(conf)

  /**
    * 判断文件是否存在
    *
    * @param pathStr
    * @return
    */
  def isExist(pathStr: String): Boolean = {
    fs.exists(new Path(pathStr))
  }

  /**
    * 获取文件夹下文件夹个数
    *
    * @param pathStr 文件夹
    * @return
    */
  def getFileNums(pathStr: String): Int = {
    val path: Path = new Path(pathStr)
    val files: Array[FileStatus] = fs.listStatus(path)
    files.length
  }

  /**
    * 保存文件
    *
    * @param pathStr hdfs 路径
    * @param rdd     存储rdd
    */
  def saveFileASText(pathStr: String, rdd: RDD[String]) = {
    //文件存在就删除
    if (isExist(pathStr)) {
      fs.delete(new Path(pathStr), true)
    }
    //文件保存
    rdd.saveAsTextFile(pathStr)
  }

  /**
    * 获取当前日期之间index 天的文件
    *
    * @param path  hdfs目录
    * @param date  当前日期
    * @param index n天前
    * @return 日期数组
    */
  def getFileOfDay(path: String, date: Date, index: Int): ArrayBuffer[String] = {
    val pathFile = new ArrayBuffer[String]
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    for (i <- 1 to index) {
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      cal.add(Calendar.DATE, -i)
      val datePath: String = dateFormat.format(cal.getTime())
      val hdfsPath: String = path + datePath
      val hdfsPath1: String = hdfsPath + "/*/*"
      if (isExist(hdfsPath)) {
        pathFile.append(hdfsPath1)
      }
    }
    pathFile
  }

  def main(args: Array[String]): Unit = {
  }
}
