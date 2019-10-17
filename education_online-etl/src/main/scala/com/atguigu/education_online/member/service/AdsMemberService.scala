package com.atguigu.education_online.member.service

import com.atguigu.education_online.member.bean.QueryResult
import com.atguigu.education_online.member.dao.DwsMemberDao
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object AdsMemberService {


  /**
    * 使用sql查询
    *
    */
  def queryDetailSql(sparkSession: SparkSession, dt: String): Unit = {

    DwsMemberDao.queryAppregurlCount(sparkSession, dt).show()
    DwsMemberDao.querySiteNameCount(sparkSession, dt).show()
    DwsMemberDao.queryAdNameCount(sparkSession, dt).show()
    DwsMemberDao.queryMemberLevelCount(sparkSession, dt).show()
    DwsMemberDao.queryVipLevelCount(sparkSession, dt).show()
    DwsMemberDao.getTop3MemberLevelPayMoneyUser(sparkSession, dt).show()

  }

  /**
    * 统计各项指标，使用api
    *
    */
  def queryDetail(sparkSession: SparkSession, dt: String): Unit = {
    import sparkSession.implicits._
    val result: Dataset[QueryResult] = DwsMemberDao.queryIdlMemberData(sparkSession).as[QueryResult].where(s"dt='${dt}'")
    result.cache()

//    统计注册来源url人数
        val value: Dataset[(String, Int)] = result.mapPartitions(partition => {
          partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt, 1))
        })
        val v2: KeyValueGroupedDataset[String, (String, Int)] = value.groupByKey(_._1)
        val v3: KeyValueGroupedDataset[String, Int] = v2.mapValues(_._2)
        val v4: Dataset[(String, Int)] = v3.reduceGroups(_ + _)

        v4.map(item => {
          val keys: Array[String] = item._1.split("_")
          val appregurl: String = keys(0)
          val dn: String = keys(1)
          val dt: String = keys(2)
          (appregurl,item._2,dt,dn)
        })
          .toDF()
          .coalesce(1)
          .write.mode(SaveMode.Overwrite)
          .insertInto("ads.ads_register_appregurlnum")

    //统计注册来源url人数
    result.mapPartitions(partition => {
      partition.map(item => (item.sitename + "_" + item.dn + "_" + item.dt, 1))
    })
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys: Array[String] = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_sitenamenum")

    //统计所属来源人数 pc mobile wechat app
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys: Array[String] = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_regsourcenamenum")

    //统计通过各广告进来的人数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_adnamenum")

    //统计各用户等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")

    //统计各用户vip等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")

    //统计各memberlevel等级 支付金额前三的用户
    import org.apache.spark.sql.functions._

    // todo result.withColumn("name",lit("张三")) 增加指定内容的一列
    // todo result.dropDuplicates("rownum") 删除重复的列 跟groupby后first比较像
    result.withColumn("rownum",row_number().over(Window.partitionBy("dn","memberlevel").orderBy(desc("paymoney"))))
      .where("rownum < 4")
      .orderBy("memberlevel","rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_top3memberpay")

  }

}
