package com.isesol.spark

import kafka.utils.ZkUtils
import kafka.serializer.Decoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import java.util.HashMap
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.streaming.kafka._

object high_streaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("this is the first spark streaming program!")
    //conf.set(key, value)
    val ssc = new StreamingContext(conf, Seconds(5))
    //ssc.checkpoint("hdfs://namenode02.isesol.com/tmp/high_streaming1")
    val zk = "datanode02.isesol.com:2181,datanode01.isesol.com:2181,hue01.isesol.com:2181/kafka"
    val group = "high_api"
    val topics = "mytest"
    val numThreads = 2
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zk, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}