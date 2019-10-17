package com.atguigu.education_online.member.controller

import com.atguigu.education_online.member.bean.DwsMember
import com.atguigu.education_online.member.service.DwsMemberService
import com.atguigu.education_online.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DwsMemberController {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[*]")

      // todo 加上并注册kryo序列化，比java的序列化要快
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .registerKryoClasses(Array(classOf[DwsMember]))

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc: SparkContext = sparkSession.sparkContext

    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

//    DwsMemberService.importMember(sparkSession,"20190722") //根据用户信息聚合用户表数据
    DwsMemberService.importMemberUseApi(sparkSession, "20190722")

    sparkSession.stop()

  }

}
