package com.atguigu.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback {
  def process(rs: ResultSet)
}

class SqlProxy {

  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
    * 修改语句
    */
  def executeUpdate(conn : Connection,sql : String,params : Array[Any]): Int = {
    var rtn = 0
    try{
      psmt = conn.prepareStatement(sql)
      if(params != null && params.length > 0) {
        for (i <- 0 until params.length){
          psmt.setObject(i + 1,params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e : Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    *
    * 查询语句
    */
  def executeQuery(conn : Connection,sql : String,params : Array[Any],queryCallback : QueryCallback): Unit = {
    rs = null
    try{
      psmt = conn.prepareStatement(sql)
      if(params != null && params.length > 0) {
        for(i <- 0 until params.length) {
          psmt.setObject(i + 1,params(i))
        }
      }
      rs = psmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

  def shutdown(conn : Connection): Unit = DataSourceUtil.closeResource(rs,psmt,conn)



}
