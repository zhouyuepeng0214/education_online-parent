package com.atguigu.qzpoint.controller

import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.atguigu.qzpoint.controller.RealtimeRegisterController.groupid
import com.atguigu.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PageJumpRateStreaming {

  private val groupid = "page_groupid"

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "30")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val topics = Array("page_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop110:9092,hadoop111:9092,hadoop112:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()

    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client: Connection = DataSourceUtil.getConnection

    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset: Long = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }


    val dsStream: DStream[(String, String, String, String, String, String, String)] = inputDStream.filter(record => {
      val data: String = record.value()
      val nObject: JSONObject = ParseJsonData.getJsonData(data)
      nObject.isInstanceOf[JSONObject]
    })
      .map(record => record.value())
      .mapPartitions(partition => {
        partition.map(item => {
          val jsonObject = ParseJsonData.getJsonData(item)
          val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
          val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
          val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
          val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
          val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
          val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
          val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
          (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
        })
      })
      .filter(item => !"".equals(item._5) && !"".equals(item._6) && !"".equals(item._7))
    dsStream.cache()

//    dsStream.foreachRDD(rdd => rdd.foreach(println))

    val pageValueDStream: DStream[(String, Int)] = dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))

    val resultDStream: DStream[(String, Int)] = pageValueDStream.reduceByKey(_ + _)
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy: SqlProxy = new SqlProxy
        val client: Connection = DataSourceUtil.getConnection
        try {
          partition.foreach(item => {
            calcPageJumpCount(sqlProxy, item, client)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    ssc.sparkContext.addFile("D:\\MyWork\\IdeaProjects\\spark\\education_online-parent\\education_online_sparkstreaming\\src\\main\\resources\\ip2region.db")
//    ssc.sparkContext.addFile("hdfs://hadoop110:9000/jars/ip2region.db")

    val ipDStream: DStream[(String, Long)] = dsStream.mapPartitions(partition => {
      val dbFile: String = SparkFiles.get("ip2region.db")
      val ipSearch = new DbSearcher(new DbConfig(), dbFile)
      partition.map(item => {
        val ip: String = item._4
        val province: String = ipSearch.memorySearch(ip).getRegion().split("\\|")(2)
        (province, 1L)
      })
    }).reduceByKey(_ + _)

//    ipDStream.foreachRDD(rdd => rdd.foreach(println))

    ipDStream.foreachRDD(rdd => {
      //查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlProxy()
      val ipClient = DataSourceUtil.getConnection
      try {
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        val history_rdd = ssc.sparkContext.makeRDD(history_data)
        val resultRdd: RDD[(String, Long)] = history_rdd.fullOuterJoin(rdd).map(item => {
          val province = item._1
          val nums: Long = item._2._1.getOrElse(0L) + item._2._2.getOrElse(0L)
          (province, nums)
        })
        resultRdd.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province = item._1
              val num = item._2
              //修改mysql数据 并重组返回最新结果数据
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num)values(?,?) on duplicate key update num=?",
                Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        val top3Rdd = resultRdd.sortBy[Long](_._2, false).take(3)
        sqlProxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        top3Rdd.foreach(item => {
          sqlProxy.executeUpdate(ipClient, "insert into top_city_num (province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(ipClient)
      }
    })





    //处理完成业务逻辑，手动提交offset维护到本地的masql中
    inputDStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy
      val client: Connection = DataSourceUtil.getConnection
      try {
        calcJumRate(sqlProxy, client) //计算转换率
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offset <- offsetRanges) {
          sqlProxy.executeUpdate(client,"replace into `offset_manager`(groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid,offset.topic,offset.partition,offset.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }

    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    *
    * 计算页面跳转个数
    *
    * @param sqlProxy
    * @param item
    * @param client
    * @return
    */
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection): Unit = {
    val keys: Array[String] = item._1.split("_")
    var num: Long = item._2
    val page_id: Int = keys(1).toInt //当前pageid
    val last_page_id = keys(0).toInt //获取上一page_id
    val next_page_id = keys(2).toInt //获取下页面page_id
    //查询当前page_id的历史num个数
    sqlProxy.executeQuery(client,"select num from page_jump_rate where page_id = ?",Array(page_id),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          num += rs.getLong(1)
        }
        rs.close()
      }
    })

    //对num 进行修改 并且判断当前page_id是否为首页
    if(page_id == 1) {
      sqlProxy.executeUpdate(client,"insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate) " +
        "values(?,?,?,?,?) on duplicate key update num = ?",Array(last_page_id,page_id,next_page_id,num,"100%",num))
    } else {
      sqlProxy.executeUpdate(client,"insert into page_jump_rate(last_page_id,page_id,next_page_id,num) " +
        "values(?,?,?,?) on duplicate key update num = ?",Array(last_page_id,page_id,next_page_id,num,num))
    }

  }

  /**
    *
    * 计算转换率
    *
    * @param sqlProxy
    * @param client
    */

  def calcJumRate(sqlProxy: SqlProxy, client: Connection) = {
    var page1_num = 0L
    var page2_num = 0L
    var page3_num = 0L

    sqlProxy.executeQuery(client,"select num from page_jump_rate where page_id=?",Array(1),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num = rs.getLong(1)
        }
        rs.close()
      }
    })

    sqlProxy.executeQuery(client,"select num from page_jump_rate where page_id=?",Array(2),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num = rs.getLong(1)
        }
        rs.close()
      }
    })

    sqlProxy.executeQuery(client,"select num from page_jump_rate where page_id=?",Array(3),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num = rs.getLong(1)
        }
        rs.close()
      }
    })

    val nf: NumberFormat = NumberFormat.getPercentInstance
    val page1ToPage2Rate: String = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate: String = if (page1_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)

    sqlProxy.executeUpdate(client,"update page_jump_rate set jump_rate = ? where page_id = ?",Array(page1ToPage2Rate,2))
    sqlProxy.executeUpdate(client,"update page_jump_rate set jump_rate = ? where page_id = ?",Array(page2ToPage3Rate,3))

  }

}
