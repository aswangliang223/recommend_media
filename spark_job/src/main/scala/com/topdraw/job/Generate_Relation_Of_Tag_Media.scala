package com.topdraw.job

import java.util

import com.topdraw.util.{CommonUtil, MySqlDataSource}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author wangliang
  * @ClassName GanSuMediaCodeTagRelation
  * @Description 甘肃少儿未打标签的MediaCode 打上标签
  * @Date 2019/5/30
  **/
object Generate_Relation_Of_Tag_Media {

  private val logger: Logger = Logger.getLogger(Generate_Relation_Of_Tag_Media.getClass)
  private val sql_mediaCode = "SELECT m.`code`FROM x_media m INNER JOIN x_media__platform mp ON mp.media_id = m.id WHERE mp.platform_id = 5 AND mp.`status` = 1 AND m.id   NOT IN (SELECT m.id FROM x_media m INNER JOIN x_media__media_tag mmt ON mmt.media_id = m.id GROUP BY m.id)"
  private val sql_tag = " SELECT tag as code FROM x_media_tag"

  private val sql_mediaCode_existe = "SELECT\n  mj.code,\n  mt.tag \nFROM\n  (SELECT\n    m.id,\n    m.`code`,\n    m.`name`\n  FROM\n    x_media m\n    INNER JOIN x_media__platform mp\n      ON mp.media_id = m.id\n  WHERE mp.platform_id = 5\n    AND mp.`status` = 1\n    AND m.id IN\n    (SELECT\n      m.id\n    FROM\n      x_media m\n      INNER JOIN x_media__media_tag mmt\n        ON mmt.media_id = m.id\n    GROUP BY m.id)) AS mj\n  INNER JOIN x_media__media_tag mmt\n    ON mj.id = mmt.media_id INNER JOIN x_media_tag mt ON mmt.media_tag_id  = mt.id "

  val outputPath = "/ai/data/gansu/out/media_tag_relation"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Generate_Relation_Of_Tag_Media").setMaster("yarn")
    conf.set("fs.default", "hdfs://ns1")
    val sc = new SparkContext(conf)

    val mediaCodeArray = getOption(sql_mediaCode)
    val tagArray = getOption(sql_tag)


    val mediaCodeRdd = sc.parallelize(mediaCodeArray.toArray)
      .map(record => {
        var arrayTag = ""
        for (i <- 0 to scala.util.Random.nextInt(15) +1) {
          arrayTag += tagArray.get(scala.util.Random.nextInt(tagArray.size())) + ","
        }
        val arraySet = arrayTag.substring(0, arrayTag.length - 1).split(",").toSet.toList.toString()
        val arrayMediaTag = arraySet.substring(5, arraySet.size - 1)
        record + "\t" + arrayMediaTag
      }).map(record => {
      (record.split("\t")(0), record.split("\t")(1).split(",").toList)
    }).flatMapValues(record => record).sortBy(record => {
      record._1
    }).map(record => {
      record._1 + "\t" + record._2
    })


    val mediaCodeArray1 = getOption1(sql_mediaCode_existe)
    val mediaCodeTagExistsRdd = sc.parallelize(mediaCodeArray1).map(record => {
      record.split(",")(0) + "\t" + record.split(",")(1)
    })

    val mediaCodeTagRelationRdd = mediaCodeRdd.union(mediaCodeTagExistsRdd).distinct().repartition(1)


    CommonUtil.saveFileASText(outputPath, mediaCodeTagRelationRdd)

  }

  def getOption(sql: String): util.ArrayList[String] = {
    val connection = MySqlDataSource.connectionPool.getConnection()
    val stmt = connection.prepareStatement(sql)
    val resultSet = stmt.executeQuery()
    val resultArray = new util.ArrayList[String]()
    while (resultSet.next()) {
      resultArray.add(resultSet.getString("code"))
    }
    stmt.close()
    connection.close()
    resultArray
  }


  def getOption1(sql: String): ArrayBuffer[String] = {
    val connection = MySqlDataSource.connectionPool.getConnection()
    val stmt = connection.prepareStatement(sql)
    val resultSet = stmt.executeQuery()
    val resultArray = new ArrayBuffer[String]()
    while (resultSet.next()) {
      resultArray.append(resultSet.getString("code") + "," + resultSet.getString("tag"))
    }
    stmt.close()
    connection.close()
    resultArray
  }
}
