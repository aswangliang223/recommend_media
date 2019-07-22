package com.topdraw.job

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.topdraw.util.CommonUtil
import org.afflatus.utility.AppConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * 计算用户标签评分，用于歌单和专题推荐
  */
object Cal_User_Tag_Score_List_Subject {
  private val logger = LoggerFactory.getLogger(Cal_User_Tag_Score_List_Subject.getClass)
  val inputPath1 = AppConfiguration.get("his_collect_score_path_out") //历史收藏 userId mediaId score
  val inputPath2 = AppConfiguration.get("media_play_score_path") // userId mediaId score
  val inputPath3 = AppConfiguration.get("mediaId_common_tag_path") // 歌单和专题推荐  单曲和common标签的关系 mediaId commonTag

  val outputPath1 = AppConfiguration.get("user_tag_score_mat_list_subject_path") //用户标签归一化评分矩阵
  val outputPath2 = AppConfiguration.get("user_index_path") // 收藏和播放日志来的用户数据
  val outputPath3 = AppConfiguration.get("user_tag_score_disperse_mat_list_subject_path") //用户标签离散化评分矩阵
  val outputPath4 = AppConfiguration.get("user_play_tag_score_mat_list_subject_path")
  val outputPath5 = AppConfiguration.get("user_collect_tag_score_mat_list_subject_path")
  val outputPath6 = AppConfiguration.get("user_play_tag_disperse_score_mat_list_subject_path") //用户标签播放离散化评分数据
  val outputPath7 = AppConfiguration.get("user_now_play_index_path")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cal_User_Tag_Score_List_Subject").setMaster("yarn").set("fs.default", "hdfs://ns1")
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
      if (CommonUtil.isExist(inputPath1) && CommonUtil.isExist(inputPath2 + datePath) && CommonUtil.isExist(inputPath3)) {
        logger.info("cal user tag score list subject start...")
        val collectRdd = sc.textFile(inputPath1 + datePath).map(record => {
          (record.split("\t")(0) + "\t" + record.split("\t")(1), record.split("\t")(2).toDouble)
        }).cache()
        val playRdd = sc.textFile(inputPath2 + datePath).map(record => {
          (record.split("\t")(0) + "\t" + record.split("\t")(1), record.split("\t")(2).toDouble)
        }).cache()
        // 用户index
        var index = 0
        val userIdIndexRdd = collectRdd.union(playRdd).repartition(1).map(record => { //index userId
          record._1.split("\t")(0)
        }).distinct().sortBy(_.toInt)
          .map(record => {
            index = index + 1
            index + "\t" + record
          })



        // userId commonTag score
        val unionRdd = collectRdd.join(playRdd).repartition(1).map(record => {
          var score: Double = 0.0
          var means: Double = 0.0
          means = 1 / 3d
          score = record._2._1.toDouble + Math.pow(record._2._2.toDouble, means)
          (record._1, score)
        }).cache()
        val resultTempRdd = collectRdd.union(playRdd).repartition(1).union(unionRdd).repartition(1).cache()

        val distinctMap = resultTempRdd.map(record => {
          val fields = record._1
          val value = record._2
          var map = mutable.Map[String, Double]()
          map += (fields -> value)
          map
        }).collect().flatten.toMap


        val distinctRdd = sc.parallelize(distinctMap.toSeq).map(record => {
          (record._1.split("\t")(1), record._1.split("\t")(0) + "\t" + record._2)
        }).cache()
        val tagRdd = sc.textFile(inputPath3).map(record => {
          (record.split("\t")(0), record.split("\t")(1)) //medidId tagName
        }).cache()

        // 用户标签总体评分
        val userLabelRdd = distinctRdd.join(tagRdd).map(record => {
          (record._2._1.split("\t")(0), record._2._2 + "\t" + record._2._1.split("\t")(1))
        }).sortBy(_._1).map(record => {
          ((record._1, record._2.split("\t")(0)), record._2.split("\t")(1).toDouble)
        }).reduceByKey(_ + _).sortBy(record => record._1._1).repartition(1).cache()
        //归一化 获取每一组最大score
        val maxScoreRdd = userLabelRdd.groupBy(item => (item._1._1))
          .map(record => {
            var maxScore: Double = 0.0
            for ((key, value) <- record._2.toMap) {
              if (value > maxScore) {
                maxScore = value
              }
            }
            (record._1, maxScore)
          }).collectAsMap()
        val userTagScoreRdd = userLabelRdd.map(record => {
          ((record._1._1, record._1._2), record._2)
        }).map(record => {
          val tmp: Double = maxScoreRdd.get(record._1._1).max
          val score = (record._2 / tmp).toFloat.formatted("%.3f")
          (record._1, score)
        }).map(record => {
          record._1._1 + "\t" + record._1._2 + "\t" + record._2
        })
        val userTagDisperseScoreRdd = userLabelRdd.map(record => {
          record._1._1 + "\t" + record._1._2 + "\t" + record._2.toDouble.formatted("%.3f")
        })

        // 单纯播放来的用户标签评分
        val userPlayTagScoreRdd = playRdd.map(record => {
          (record._1.split("\t")(1), record._1.split("\t")(0) + "\t" + record._2)
        }).join(tagRdd).repartition(1).map(record => {
          (record._2._1.split("\t")(0), record._2._2 + "\t" + record._2._1.split("\t")(1))
        }).sortBy(_._1).map(record => {
          ((record._1, record._2.split("\t")(0)), record._2.split("\t")(1).toDouble)
        }).reduceByKey(_ + _).sortBy(record => record._1._1).cache()
        val maxScoreUserPlayRdd = userPlayTagScoreRdd.groupBy(item => (item._1._1)).map(record => {
          var maxScore: Double = 0.0
          for ((key, value) <- record._2.toMap) {
            if (value > maxScore) {
              maxScore = value
            }
          }
          (record._1, maxScore)
        }).collectAsMap()
        val userPlayTagScoreResult = userPlayTagScoreRdd.map(record => {
          ((record._1._1, record._1._2), record._2)
        }).map(record => {
          val tmp: Double = maxScoreUserPlayRdd.get(record._1._1).max
          val score = (record._2 / tmp).toFloat.formatted("%.3f")
          (record._1, score)
        }).map(record => {
          record._1._1 + "\t" + record._1._2 + "\t" + record._2
        })


        //单纯收藏来的用户评分
        val userCollectTagScoreRdd = collectRdd.map(record => {
          (record._1.split("\t")(1), record._1.split("\t")(0) + "\t" + record._2)
        }).join(tagRdd).repartition(1).map(record => {
          (record._2._1.split("\t")(0), record._2._2 + "\t" + record._2._1.split("\t")(1))
        }).sortBy(_._1).map(record => {
          ((record._1, record._2.split("\t")(0)), record._2.split("\t")(1).toDouble)
        }).reduceByKey(_ + _).sortBy(record => record._1._1).cache()
        val maxScoreUserCollectRdd = userCollectTagScoreRdd.groupBy(item => (item._1._1)).map(record => {
          var maxScore: Double = 0.0
          for ((key, value) <- record._2.toMap) {
            if (value > maxScore) {
              maxScore = value
            }
          }
          (record._1, maxScore)
        }).collectAsMap()

        val userCollectTagScoreResult = userCollectTagScoreRdd.map(record => {
          ((record._1._1, record._1._2), record._2)
        }).map(record => {
          val tmp: Double = maxScoreUserCollectRdd.get(record._1._1).max
          val score = (record._2 / tmp).toFloat.formatted("%.3f")
          (record._1, score)
        }).map(record => {
          record._1._1 + "\t" + record._1._2 + "\t" + record._2
        })


        val userPlayTagDisperseScoreRdd = userPlayTagScoreRdd.map(record =>{
          record._1._1 + "\t" + record._1._2 + "\t" + record._2.toDouble.formatted("%.3f")
        }).cache()

        var index1 = 0
        val userNowIndexRdd = playRdd.map(record => {
          record._1.split("\t")(0)
        }).distinct().sortBy(_.toInt).repartition(1).map(record => {
          index1 = index1 + 1
          index1 + "\t" + record
        })
        logger.info("cal user tag score list subject success...")
        CommonUtil.saveFileASText(outputPath1 + datePath, userTagScoreRdd)
        CommonUtil.saveFileASText(outputPath2 + datePath, userIdIndexRdd)
        CommonUtil.saveFileASText(outputPath3 + datePath, userTagDisperseScoreRdd)
        CommonUtil.saveFileASText(outputPath4 + datePath, userPlayTagScoreResult)
        CommonUtil.saveFileASText(outputPath5 + datePath, userCollectTagScoreResult)
        CommonUtil.saveFileASText(outputPath6 + datePath, userPlayTagDisperseScoreRdd)
        CommonUtil.saveFileASText(outputPath7 + datePath, userNowIndexRdd)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("cal user tag score list subject error【" + ex.getMessage + "】")
      }
    } finally {
      sc.stop()
    }
  }
}
