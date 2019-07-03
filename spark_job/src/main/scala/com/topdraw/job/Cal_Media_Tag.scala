package com.topdraw.job

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.topdraw.util.CommonUtil
import org.afflatus.utility.AppConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
  * 歌曲标签总数据 mediaId common_artist_tag
  * 标签index index tag（commontag & artisttag）
  * 艺术家标签取前两个
  */
object Cal_Media_Tag {

  private val logger = LoggerFactory.getLogger(Cal_Media_Tag.getClass)
  private val mediaId_common_tag_path = AppConfiguration.get("mediaId_common_tag_path")
  private val mediaId_artist_tag_path = AppConfiguration.get("mediaId_artist_tag_path")
  private val common_artist_tag_index_path_out = AppConfiguration.get("common_artist_tag_index_path_out")
  private val mediaId_common_artist_tag_path = AppConfiguration.get("mediaId_common_artist_tag_path")
  private val common_artist_tag_index_path_in = AppConfiguration.get("common_artist_tag_index_path_in")
  private val mediaId_index_path = AppConfiguration.get("mediaId_index_path")


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cal_Media_Tag").setMaster("yarn").set("fs.default", "hdfs://ns1")
    val sc = new SparkContext(conf)
    try {
      var date = new Date()
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      cal.add(Calendar.DATE, 0)
      date = cal.getTime()
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val datePath = dateFormat.format(date)
      if (CommonUtil.isExist(mediaId_common_tag_path) && CommonUtil.isExist(mediaId_artist_tag_path) && CommonUtil.isExist(common_artist_tag_index_path_in)) {
        logger.info("cal media tag start...")
        val commonTagRdd = sc.textFile(mediaId_common_tag_path).filter(record => {
          record.split("\t").length == 2
        }).map(record => {
          val row = record.split("\t")
          (row(0), row(1))
        })
        val artistTagRdd = sc.textFile(mediaId_artist_tag_path)
          .map(_.replaceAll("&", "、"))
          .map(_.replaceAll(",", "、"))
          .filter(record => {
            record.split("\t").length == 2
          })
          .map(record => {
            val row = record.split("\t")
            (row(0), row(1))
          })
        val array = new ArrayBuffer[(String, String)]
        for ((key, value) <- artistTagRdd.collect()) {
          var arrs = value.split("、")
          if (arrs.length > 2) {
            //这里获取分割的主要标签 前两个
            arrs = arrs.take(2)
          }
          for (arr <- arrs) {
            array.append((key, arr))
          }
        }
        val artistSplitRdd = sc.parallelize(array)
        val userMediaTagRdd = commonTagRdd.union(artistSplitRdd).repartition(1).
          map(record => {
            record._1 + "\t" + record._2
          }).distinct().repartition(1).sortBy(record => record.split("\t")(0))
        var index1 = 0
        val mediaIdRdd = userMediaTagRdd.map(record => {
          record.split("\t")(0)
        }).distinct().map(record => {
          index1 += 1
          index1 + "\t" + record
        })

        // artist and common tag index
        val common_artist_tag_index_rdd = sc.textFile(common_artist_tag_index_path_in)
          .map(_.replaceAll("&", "、"))
          .map(_.replaceAll(",", "、"))
          .collect()
        val arr = new ArrayBuffer[String]()
        for (value <- common_artist_tag_index_rdd) {
          breakable {
            // 去掉多余的换行
            if ("".equals(value)) {
              break()
            } else {
              arr.append(value)
            }
          }
        }
        val com_artsistRdd = sc.parallelize(arr).filter(record => {
          //过滤掉 lenth 不等于 2 的数据
          record.split("\t").length == 2
        })
          .map(record => {
            record.split("\t")(1)
          })
        val tagArray = new ArrayBuffer[String]()
        for (value <- com_artsistRdd.collect()) {
          val array = value.split("、")
          // 这里得到 分割的所有标签
          for (arr <- array) {
            tagArray.append(arr)
          }
        }
        var index = 0
        val tagIndexRdd = sc.parallelize(tagArray).distinct()
          .map(record => {
            index += 1
            index + "\t" + record
          }).repartition(1)
        CommonUtil.saveFileASText(mediaId_common_artist_tag_path + datePath, userMediaTagRdd)
        CommonUtil.saveFileASText(common_artist_tag_index_path_out + datePath, tagIndexRdd)
        CommonUtil.saveFileASText(mediaId_index_path+datePath,mediaIdRdd)
      }
      else {
        logger.error("file does not exists")
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("calMediaTag job error【" + ex.getMessage + "】")
      }
    } finally {
      sc.stop()
    }
  }
}
