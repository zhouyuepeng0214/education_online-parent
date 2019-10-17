package com.atguigu.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","D:\\learning\\hadoop-2.7.2")

    val conf: SparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("input/register.log", 10)
      .foreachPartition(partition => {
        val properties = new Properties()
        properties.put("bootstrap.servers","hadoop110:9092,hadoop111:9092,hadoop113:9092")
        properties.put("acks", "1")
        properties.put("batch.size", "16384")
        properties.put("linger.ms", "10")
        properties.put("buffer.memory", "33554432")
        properties.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)
        partition.foreach(item => {
          val msg = new ProducerRecord[String,String]("register_topic",item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })

    sc.stop()

  }

}
