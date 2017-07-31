package com.isesol.kafka
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.kafka.clients._
import org.apache.kafka.common._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

object spark_kafka extends Logging {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
  //  ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    words.print() 
    

    ssc.start()
    ssc.awaitTermination()
  }
}