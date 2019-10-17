package com.atguigu.education_online.member.dao

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object DwsMemberDao {

  /**
    *
    * 查询宽表数据
    */
  def queryIdlMemberData(sparkSession: SparkSession): sql.DataFrame ={
    sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) paymoney,dt,dn from dws.dws_member")
  }

  //统计所属网站人数
  def queryAppregurlCount(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select appregurl,dn,dt,count(uid) uid_count from dws.dws_member where dt='${dt}' group by appregurl,dn,dt")
  }

  //统计所属来源人数
  def querySiteNameCount(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select regsourcename,dn,dt,count(uid) uid_count from dws.dws_member where dt='${dt}' group by regsourcename,dn,dt")
  }

  //统计通过各广告注册的人数
  def queryAdNameCount(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select adname,dn,dt,count(uid) uid_count from dws.dws_member where dt='${dt}' group by adname,dn,dt")
  }

  //统计各用户等级人数
  def queryMemberLevelCount(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select memberlevel,dn,dt,count(uid) uid_count from dws.dws_member where dt='${dt}' group by memberlevel,dn,dt")
  }

  //统计各用户VIP登记人数
  def queryVipLevelCount(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select vip_level,dn,dt,count(uid) uid_count from dws.dws_member where dt='${dt}' group by vip_level,dn,dt")
  }

  //统计各memberlevel等级 支付金额前三的用户

  def getTop3MemberLevelPayMoneyUser(sparkSession: SparkSession,dt : String) : sql.DataFrame = {
    sparkSession.sql(s"select * from( select memberlevel,uid,ad_id,register,appregurl,regsource,regsourcename,adname, siteid," +
      s"sitename,vip_level,cast(paymoney as decimal(10,4)), row_number() over(partition by memberlevel,dn order by " +
      s"cast(paymoney as decimal(10,4)) desc) rk,dn from dws.dws_member where dt='${dt}') t where rk <= 3 order by memberlevel,rk")
  }


}
