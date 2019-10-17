package com.atguigu.education_online.qz.controller

import com.atguigu.education_online.qz.dao.{DwdQzDao, DwsQzDao}
import com.atguigu.education_online.qz.service.DwsQzService
import com.atguigu.education_online.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DwsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setAppName("dws_qz_controller").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.useSnappyCompression(sparkSession)

    val dt = "20190722"

    DwsQzService.saveDwsQzChapter(sparkSession, dt)
    DwsQzService.saveDwsQzCourse(sparkSession, dt)
    DwsQzService.saveDwsQzMajor(sparkSession, dt)
    DwsQzService.saveDwsQzPaper(sparkSession, dt)
    DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)

//    DwsQzDao.getDwsQzChapter(sparkSession,dt).limit(5).show()
//    DwsQzDao.getDwsQzCourse(sparkSession,dt).limit(5).show()
//    DwsQzDao.getDwsQzMajor(sparkSession,dt).limit(5).show()
//    DwsQzDao.getDwsQzPaper(sparkSession,dt).limit(5).show()
//    DwsQzDao.getDwsQzQuestion(sparkSession,dt).limit(5).show()




    sparkSession.stop()


  }

}
