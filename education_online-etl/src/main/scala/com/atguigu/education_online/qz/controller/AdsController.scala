package com.atguigu.education_online.qz.controller

import com.atguigu.education_online.qz.service.{AdsQzService, DwsQzService}
import com.atguigu.education_online.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AdsController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setAppName("dws_qz_controller").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession)


    val dt = "20190722"

//    AdsQzService.getTarget(sparkSession,dt)
    AdsQzService.getTargetApi(sparkSession,dt)


    sparkSession.stop()


  }

}
