package com.topdraw.job

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import com.topdraw.util.CommonUtil
import org.afflatus.utility.AppConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}

/**
  * 统计30天的播放数据得分
  */
object Cal_Media_Play_One_Month_Score {

  private val logger = LoggerFactory.getLogger(Cal_Media_Play_Score.getClass)
  private val inputPath = AppConfiguration.get("base_hdfs_path")
  private val outputPath1 = AppConfiguration.get("media_play_score_one_month_path")
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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cal_Media_Play_One_Month_Score").setMaster("yarn").set("fs.default", "hdfs://ns1")
    val sc = new SparkContext(conf)
    try {
      val hdfsFile = CommonUtil.getFileOfDay(inputPath, new Date(), 30)
      if (hdfsFile.length > 0) {
        logger.info("cal_media_play_score job Start-----------------------------------" + datePath + "-------------------------")
        val transRdd = sc.textFile(hdfsFile.mkString(",")).map(record => {
          JSON.parseObject(record)
        }).filter(record => {
          (record.containsValue("MediaSeek") ||
            record.containsValue("MediaPause") ||
            record.containsValue("MediaPlay") ||
            record.containsValue("MediaStop")) &&
            record.get("userId") != -1 &&
            record.get("duration") != 0 &&
            record.get("mediaId") != null &&
            record.get("userId") != null &&
            record.get("mediaId") != -1 &&
            record.containsKey("actionTime")
        }).map(record => {
          ((record.get("userId"), record.get("mediaId")), record)
        }).groupByKey()
        //计算每一个用户对每一个media 的有效观看时长 ((userId,mediaId),record)
        // 加入 播放衰减系数（播放次数）
        val effectiveTimeRdd1 = transRdd.filter(record => {
          var isEffectivePlay = false
          val array = record._2.toArray
          var nums = 0
          for (i <- 0 to array.length - 1) {
            breakable {
              if ("MediaPlay".equals(array(i).get("action"))) {
                nums += 1
                isEffectivePlay = true
              }
              if (isEffectivePlay) break()
            }
          }
          if (nums == 1 && "MediaPlay".equals(array(array.length - 1).get("action"))) {
            isEffectivePlay = false
          }
          isEffectivePlay
        })
        val effectiveTimeRdd = effectiveTimeRdd1.map(record => {
          val userId = record._1._1
          val mediaId = record._1._2
          val array = record._2.toArray.sortBy(_.get("actionTime").toString())
          var effectiveTime = 0
          var duration = 0
          var sPosition = 0
          var ePosition = 0
          var location = "S"
          var perAction = ""
          var play_count = 0
          var play_rating = 0.0 // 每一首曲子的播放率
          for (arr <- array) {
            val action = arr.get("action")
            if ("MediaPlay".equals(action)) {
              perAction = "MediaPlay"
            }
            if ("MediaStop".equals(action) && "MediaPlay".equals(perAction)) {
              play_count += 1
              perAction = ""
            }
            if ("S".equals(location)) {
              if ("MediaPlay".equals(action)) {
                sPosition = arr.get("position").toString.toInt
                location = "E"
              }
            } else if ("E".equals(location)) {
              if ("MediaSeek".equals(action)) {
                if (arr.containsKey("startTime") && arr.containsKey("endTime")) {
                  ePosition = arr.get("startTime").toString.toInt
                  if (ePosition >= sPosition) {
                    val tempEffectiveTime = ePosition - sPosition
                    effectiveTime += tempEffectiveTime
                    duration = arr.get("duration").toString.toInt
                    val temp_play_rating = tempEffectiveTime / duration.toFloat
                    play_rating += temp_play_rating
                    //重新赋值
                    location = "E"
                    ePosition = 0
                    sPosition = arr.get("endTime").toString.toInt
                  }
                }
              } else if ("MediaPause".equals(action) || "MediaStop".equals(action)) {
                if (arr.containsKey("position")) {
                  ePosition = arr.get("position").toString.toInt
                  if (ePosition >= sPosition) {
                    val tempEffectiveTime = ePosition - sPosition
                    effectiveTime += tempEffectiveTime
                    duration = arr.get("duration").toString.toInt
                    val temp_play_rating = tempEffectiveTime / duration.toFloat
                    play_rating += temp_play_rating
                    //重新赋值
                    location = "S"
                    sPosition = 0
                    ePosition = 0
                  }
                }
              }
            }
          }
          (userId, mediaId, effectiveTime, duration, play_count, play_rating)
        }).filter(record => {
          record._3 != 0 && record._6 != 0
        }).cache()
        val scoreRdd = effectiveTimeRdd.map(record => {
          val score = calculateScore(record._3, record._4, record._5)
          (record._1 + "\t" + record._2 + "\t" + score + "\t" + record._5 + "\t" + record._6)
        })
        CommonUtil.saveFileASText(outputPath1 + datePath, scoreRdd.coalesce(1,true))
      } else {
        logger.warn("cal_media_play_score job warn--------------------------------------未找到HDFS文件:" + hdfsFile)
      }
      logger.info("cal_media_play_score job end----------------------------------------------------------------")
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("cal_media_play_score job error【" + ex.getMessage + "】")
      }
    } finally {
      sc.stop()
    }
  }


  /**
    * 计算播放得分
    *
    * @param playDur
    * @param preDuration
    * @return
    */
  def calculateScore(playDur: Int, preDuration: Int, playCount: Int): Double = {
    var res: Double = 0
    if (playDur == 0 || preDuration == 0) {
      res = 0.5
    } else {
      val playDur1: Double = playDur.toDouble
      val preDuration1: Double = preDuration.toDouble
      res = Math.abs(preDuration1 - playDur1) / (2 * preDuration1) + 0.5
    }
    res
  }

}
