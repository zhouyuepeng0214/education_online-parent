package com.atguigu.education_online.qz.controller

import com.atguigu.education_online.qz.service.EtlDataService
import com.atguigu.education_online.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdController {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")

    val conf: SparkConf = new SparkConf().setAppName("DwdController").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    HiveUtil.openDynamicPartition(sparkSession)

//    EtlDataService.etlQzChapter(ssc,sparkSession)
//    EtlDataService.etlQzChapterList(ssc,sparkSession)
//    EtlDataService.etlQzPoint(ssc, sparkSession)
//    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
//    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
//    EtlDataService.etlQzCourse(ssc, sparkSession)
//    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
//    EtlDataService.etlQzWebsite(ssc, sparkSession)
//    EtlDataService.etlQzMajor(ssc, sparkSession)
//    EtlDataService.etlQzBusiness(ssc, sparkSession)
//    EtlDataService.etlQzPaperView(ssc, sparkSession)
//    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
//    EtlDataService.etlQzPaper(ssc, sparkSession)
//    EtlDataService.etlQzCenter(ssc, sparkSession)
//    EtlDataService.etlQzQuestion(ssc, sparkSession)
//    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)


    sparkSession.stop()
    ssc.stop()

  }

}
