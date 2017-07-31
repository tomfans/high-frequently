package com.isesol.func
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Calendar

object map {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("this is my first app")
    val sc = new SparkContext(conf)
    val wordcount = sc.parallelize(List(1, 2, 3))

    wordcount.map(x => x + 1).foreach { x => println(x) }

  //  val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1, 1, 1))
 //   val c = b.countByValue()
 //   println(c.get(1))
    
   // val a = Long.MaxValue - cal.getTimeInMillis()

  }
}