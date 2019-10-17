package com.atguigu.education_online.util

import org.apache.spark.sql.SparkSession

object HiveUtil {
  /**
    * 使用snappy压缩,spark默认支持
    * @param sparkSession
    */
  def useSnappyCompression(sparkSession: SparkSession): Unit = {
    sparkSession.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    sparkSession.sql("set mapreduce.output.fileoutputformat.compress=true")
    sparkSession.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }

  /**
    * 开启压缩
    * @param sparkSession
    */
  def openCompression(sparkSession: SparkSession) = {
    sparkSession.sql("set mapred.output.compress=true")
    sparkSession.sql("set hive.exec.compress.output=true")

  }


  /**
    * 开启动态分区，非严格模式
    * @param sparkSession
    */
  def openDynamicPartition(sparkSession: SparkSession) = {
    sparkSession.sql("set hive.exec.dynamic.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

}
