package com.atguigu.qzpoint.controller


import java.lang
import java.sql.{Connection, ResultSet}

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable


object RealtimeRegisterController {

  private val groupid = "register_group_test"

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop110:9092,hadoop111:9092,hadoop112:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    ssc.checkpoint("checkpoint")
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

    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap,offsetMap))
    }

    val resultDStream: DStream[(String, Int)] = inputDStream.filter(record => record.value().split("\t").length == 3)
      .mapPartitions(partition => {
        partition.map(record => {
          val register: StringOps = record.value()
          val registerArray: mutable.ArrayOps[String] = register.split("\t")
          val app_name: String = registerArray(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })

    resultDStream.cache()
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum //本批次求和
      val previousCount: Int = state.getOrElse(0) // 历史数据
      Some(currentCount + previousCount)
    }

    resultDStream.updateStateByKey(updateFunc).print()

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    inputDStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try{
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for(offset <- offsetRanges) {
          sqlProxy.executeUpdate(client,"replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid,offset.topic,offset.partition,offset.untilOffset))
        }
      } catch {
        case e : Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
