package com.atguigu.education_online.member.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.education_online.member.bean._
import com.atguigu.education_online.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {

  /**
    * 1.
    * etl用户注册信息
    * @param ssc
    * @param sparkSession
    */
  def etlMemberRegtypeLog(ssc : SparkContext,sparkSession : SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换

    ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/memberRegtype.log")
      .filter(item =>{
        val obj: JSONObject = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition =>{
        partition.map(item => {
          val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
//          val dwsMemberRegtype: DwsMemberRegtype = jsonObject.asInstanceOf[DwsMemberRegtype]
//          dwsMemberRegtype.regsourcename = dwsMemberRegtype.regsource.match{
//            case "1" => "PC"
//            case "2" => "Mobile"
//            case "3" => "App"
//            case "4" => "WeChat"
//            case _ => "other"
//          }
//          dwsMemberRegtype
          val appkey: String = jsonObject.getString("appkey")
          val appregurl = jsonObject.getString("appregurl")
          val bdp_uuid = jsonObject.getString("bdp_uuid")
          val createtime = jsonObject.getString("createtime")
          val domain = jsonObject.getString("webA")
          val isranreg = jsonObject.getString("isranreg")
          val regsource = jsonObject.getString("regsource")
          val regsourceName = regsource match{
            case "1" => "PC"
            case "2" => "Mobile"
            case "3" => "App"
            case "4" => "WeChat"
            case _ => "other"
          }
          val uid = jsonObject.getIntValue("uid")
          val websiteid = jsonObject.getIntValue("websiteid")
          val dt = jsonObject.getString("dt")
          val dn = jsonObject.getString("dn")
          (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
        })
      })
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype") //todo 有日期，overwrite
      // todo saveAsTable 自动创建表，若hive中有表则报错，根据字段名匹配
    // todo insertInto 导数据到现有表，根据列名顺序匹配
  }

  /**
    * 2.
    * etl用户表数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/member.log")
      .filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
//        jsonObject.asInstanceOf[DwdMember]
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val email = jsonObject.getString("email")
        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getIntValue("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, dt, dn)
      })
    }).toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
  }

  /**
    * 3.
    * 导入广告表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    val result = ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/baseadlog.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
//        jsonObject.asInstanceOf[DwdBaseadlog]
        val adid = jsonObject.getIntValue("adid")
        val adname = jsonObject.getString("adname")
        val dn = jsonObject.getString("dn")
        (adid, adname, dn)
      })
    })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  /**
    * 4.
    * 导入网站表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/baswewebsite.log")
      .filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    })
      .mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
//        jsonObject.asInstanceOf[DwdBaswewebsite]
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }


  /**
    * 5.
    * 导入用户付款信息
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/pcentermempaymoney.log")
      .filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    })
      .mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
//        jSONObject.asInstanceOf[DwdPcentermempaymoney]
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val dt = jSONObject.getString("dt")
        val dn = jSONObject.getString("dn")
        (uid, paymoney, site_id, vip_id, dt, dn)
      })
    })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Append)
      .insertInto("dwd.dwd_pcentermempaymoney")

  }

  /**
    * 6.
    * 导入用户vip基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop110:9000/user/atguigu/ods/pcenterMemViplevel.log")
      .filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    })
      .mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
//        jSONObject.asInstanceOf[DwdPcenterMemViplevel]
        val discountval = jSONObject.getString("discountval")
        val end_time = jSONObject.getString("end_time")
        val last_modify_time = jSONObject.getString("last_modify_time")
        val max_free = jSONObject.getString("max_free")
        val min_free = jSONObject.getString("min_free")
        val next_level = jSONObject.getString("next_level")
        val operator = jSONObject.getString("operator")
        val start_time = jSONObject.getString("start_time")
        val vip_id = jSONObject.getIntValue("vip_id")
        val vip_level = jSONObject.getString("vip_level")
        val dn = jSONObject.getString("dn")
        (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
      })
    })
      .toDF().coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_vip_level")
  }




}
