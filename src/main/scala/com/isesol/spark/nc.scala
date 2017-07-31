package com.isesol.spark

import kafka.serializer.Decoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
object nc {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("this is the first spark streaming program!")
    //conf.set(key, value)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://nameservice1/tmp/high_streaming")
    val lines = ssc.socketTextStream("hue01.isesol.com", 9999)
    val words = lines.flatMap { x => x.split(" ") }
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}