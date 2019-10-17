package com.atguigu.education_online.member.controller

import com.atguigu.education_online.member.dao.DwsMemberDao
import com.atguigu.education_online.member.service.AdsMemberService
import com.atguigu.education_online.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsMemberController {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)

//    AdsMemberService.queryDetailSql(sparkSession,"20190722")

    //使用api
    AdsMemberService.queryDetail(sparkSession,"20190722")






    ssc.stop()

  }

}
