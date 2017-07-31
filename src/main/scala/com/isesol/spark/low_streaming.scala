package com.isesol.spark

import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.log4j._
import kafka.serializer.Decoder
import kafka.serializer.StringDecoder
import kafka.message._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import java.util.HashMap
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.streaming.kafka._

object low_streaming {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[2]").setAppName("this is the first spark streaming program!")
    //conf.set(key, value)
    val ssc = new StreamingContext(conf, Seconds(5))
    //ssc.checkpoint("hdfs://namenode02.isesol.com/tmp/high_streaming1")
    val zk = "datanode02.isesol.com:2181,datanode01.isesol.com:2181,hue01.isesol.com:2181/kafka"
    val brokers = "yarn02.isesol.com:9092,yarn01.isesol.com:9092,hue01.isesol.com:9092"
    val group = "low_api"
    val topics = "mytest"
    val topicsSet = topics.split(",").toSet
    val numThreads = 2
    var offsetRanges = Array[OffsetRange]()
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> group, "auto.offset.reset" -> "smallest")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.foreachRDD{rdd => 
    for (offsets <- offsetRanges) {println(s"${offsets.topic} ${offsets.partition} ${offsets.fromOffset} ${offsets.untilOffset}")}
    val line = rdd.map(x => x._2)
    val word = line.flatMap { x => x.split(" ") }
    val wordCounts = word.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.foreach(println)
    }
    // val group_id = kafkaParams.get("group.id").get
    //val word = words.flatMap(_.split(" "))
    // val wordCounts = word.map(x => (x, 1L)).reduceByKey(_ + _)
    //wordCounts.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}