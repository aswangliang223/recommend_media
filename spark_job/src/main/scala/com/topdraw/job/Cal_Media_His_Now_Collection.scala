package com.topdraw.job

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import com.topdraw.util.CommonUtil
import org.afflatus.utility.AppConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object Cal_Media_His_Now_Collection {
  private val logger: Logger = LoggerFactory.getLogger(Cal_Media_His_Now_Collection.getClass)
  val inputPath: String = AppConfiguration.get("base_hdfs_path")
  val his_collect_in_path: String = AppConfiguration.get("mediaId_collect_path")
  val historyCollectPath: String = AppConfiguration.get("his_collect_score_path_out")

  /**
    * 收藏歌曲打分 : 每天收藏打分，历史收藏打分（获取的是最新一天的收藏数据去做收藏播放的整体评分）
    *
    * @param args
    * 输入数据格式 ：JSON串 {""action"":""MediaUncollect""","createtime:""2018-12-13T21:02:21+08:00""",
    * "product:""Melody""","sessionId:""e6e320d6-8557-4fb0-ae93-25dbd55303e7""","ui:""CategoryList""",
    * userId:1,"appId:""test001""","version:""2.3.2-beta""",media:8839}
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cal_Media_His_Now_Collection").setMaster("yarn")
    conf.set("fs.default", "hdfs://ns1")
    val sc = new SparkContext(conf)
    try {
      var date: Date = new Date()
      val cal: Calendar = Calendar.getInstance()
      cal.setTime(date)
      cal.set(Calendar.HOUR_OF_DAY, 0)
      cal.set(Calendar.MINUTE, 0)
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      cal.add(Calendar.DATE, 0)
      date = cal.getTime
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val datePath: String = dateFormat.format(date)
      val hdfsFile = CommonUtil.getFileOfDay(inputPath, new Date(), 7)
      if (!(CommonUtil.isExist(his_collect_in_path) && hdfsFile.length > 0)) {
        logger.error("inputPath does not exist")
      } else {
        logger.info("cal media history collection start...")
        val transRdd = sc.textFile(hdfsFile.mkString(",")).map(record =>
          JSON.parseObject(record))
        //收藏数据
        val collectRdd = transRdd.filter(record => {
          "MediaCollect".equals(record.get("action").toString)
        }).map(record => {
          (record.get("userId") + "\t" + record.get("mediaId"), 1)
        }).cache()
        collectRdd.foreach(println)
        //取消收藏数据
        val unCollectRdd = transRdd.filter({ record =>
          "MediaUnCollect".equals(record.get("action").toString)
        }).map(record => {
          (record.get("userId") + "\t" + record.get("mediaId"), -1)
        }).cache()
        unCollectRdd.foreach(println)
        //过滤掉当天收藏又取消的用户
        val unionRdd = collectRdd.union(unCollectRdd).reduceByKey(_ + _).filter(record => {
          record._2 != 0
        }).cache()


        // 能否得到收藏的历史数据
        //假如历史收藏数据存在
        val historyCollectRdd = sc.textFile(his_collect_in_path).map(record => {
          JSON.parseObject(record)
        }).map(record => {
          (record.get("userId") + "\t" + record.get("mediaId"), 1)
        }).cache()

        //当天的收藏信息和 历史收藏 union
        val unionHistoryRdd = historyCollectRdd.union(unionRdd).reduceByKey(_ + _).filter(record => {
          record._2 == 1
        })
        //添加评分（历史收藏打分）
        val historyGradeRdd = unionHistoryRdd.map(record => { //userId \t mediaId \t score
          record._1 + "\t" + "2.0" //收藏的基础评分 2.0
        }).repartition(1) //设置repartition的个数为1
        CommonUtil.saveFileASText(historyCollectPath + datePath, historyGradeRdd)
        logger.info("cal media history collection success!!!")
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("cal media history collection error 【" + ex.getMessage + "】")
      }
    } finally {
      sc.stop()
    }
  }
}
