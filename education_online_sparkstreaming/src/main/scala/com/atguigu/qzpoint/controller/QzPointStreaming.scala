package com.atguigu.qzpoint.controller

import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


import scala.collection.mutable


object QzPointStreaming {

  private val groupid = "qz_point_group"

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val topics = Array("qz_log")
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
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap,offsetMap))
    }

    inputDStream.foreachRDD(rdd => rdd.foreach(println))

    //过滤不正常数据，获取数据
    val dsStream: DStream[(String, String, String, String, String, String)] = inputDStream.filter(record => record.value().split("\t").length == 6)
      .mapPartitions(partition => {
        partition.map(record => {
          val dataArr: Array[String] = record.value().split("\t")
          val uid: String = dataArr(0)
          val courseid: String = dataArr(1)
          val pointid: String = dataArr(2)
          val questionid: String = dataArr(3)
          val istrue: String = dataArr(4)
          val createTime: String = dataArr(5)
          (uid, courseid, pointid, questionid, istrue, createTime)
        })
      })

//    dsStream.foreachRDD(rdd => rdd.foreach(println))

    dsStream.foreachRDD(rdd => {
      //获取相同用户 同一课程 同一知识点的数据
      val groupRDD: RDD[(String, Iterable[(String, String, String, String, String, String)])] =
        rdd.groupBy(item => item._1 + "_" + item._2 + "_" + item._3)
      groupRDD.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy: SqlProxy = new SqlProxy()
        val client: Connection = DataSourceUtil.getConnection
        try{
          partition.foreach{
            case (key,iters) => qzQuestionUpdate(key,iters,sqlProxy,client)
          }
        } catch {
          case e : Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    inputDStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offset <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, offset.topic, offset.partition, offset.untilOffset))
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
    * 对提目标更新
    *
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    * @return
    */

  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)],
                       sqlProxy: SqlProxy, client: Connection): Int = {
    val keys: Array[String] = key.split("_")
    val userid: Int = keys(0).toInt
    val courseid: Int = keys(1).toInt
    val pointid: Int = keys(2).toInt
    val array: Array[(String, String, String, String, String, String)] = iters.toArray
    val questionids: Array[String] = array.map(_._4).distinct //对当前批次的数据下questionid 去重
    //查询历史数据下的 questionid
    var questionids_history: Array[String] = Array()
    sqlProxy.executeQuery(client,"select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(userid,courseid,pointid),new QueryCallback {
      override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
          }
          rs.close()
      }
    })
    //获取到历史数据后再与当前数据进行拼接 去重
    val resultQuestionid: Array[String] = questionids.union(questionids_history).distinct
    val countSize: Int = resultQuestionid.length
    val resultQuestionid_str: String = resultQuestionid.mkString(",")
    val qzCount: Int = questionids.length  //去重后的题个数
    var qzSum: Int = array.length  //获取当前批次题总数
    var qzIstrueCount: Int = array.filter(item => "1".equals(item._5)).size  //获取当前批次做正确的题个数
    val createTime: String = array.map(_._6).min  //获取最早的创建时间 作为表中创建时间
    //更新qz_point_set 记录表 此表用于存当前用户做过的questionid表
    // todo JDK1.8的 是线程安全的
    val updateTime: String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    sqlProxy.executeUpdate(client,"insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      " on duplicate key update questionids=?,updatetime=?",
      Array(userid,courseid,pointid,resultQuestionid_str,createTime,createTime,resultQuestionid_str,updateTime))

    var qzSumHistory = 0
    var isTrueHistory = 0
    sqlProxy.executeQuery(client,"select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid,courseid,pointid),new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while(rs.next()){
            qzSumHistory += rs.getInt(1)
            isTrueHistory += rs.getInt(2)
          }
          rs.close()
        }
      })
    qzSum += qzSumHistory
    qzIstrueCount += isTrueHistory
    val correct_rate: Double = qzIstrueCount.toDouble / qzSum
    //计算完成率
    //假设每个知识点下一共有30道题  先计算题的做题情况 再计知识点掌握度
    val qz_detail_rate: Double = countSize.toDouble / 30 //算出做题情况乘以 正确率 得出完成率 假如30道题都做了那么正确率等于 知识点掌握度
    val mastery_rate: Double = qz_detail_rate * correct_rate

    sqlProxy.executeUpdate(client,"insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue," +
      "correct_rate,mastery_rate,createtime,updatetime) values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?," +
      "qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qzSum, countSize, qzIstrueCount, correct_rate, mastery_rate, createTime,
        updateTime, qzSum, countSize, qzIstrueCount, correct_rate, mastery_rate, updateTime))





  }

}
