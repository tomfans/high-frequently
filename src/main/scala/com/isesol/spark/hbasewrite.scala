package com.isesol.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.client.HTable

object hbasewrite extends Serializable {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("this is for hbase write")
    //conf.setSparkHome("d:\\spark_home")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    val tablename = "bank"
    /* val jobConf = new JobConf(hbaseconf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename) */
    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val bankRDD = sc.textFile("hdfs://namenode01.isesol.com:8020/tmp/bank.csv").map { x => x.split(";") }

    // val myTable = new HTable(hbaseconf, TableName.valueOf(tablename))
    /*   val rdd = bankRDD.map(_.split(';')).map { arr =>
      {
        val put = new Put(Bytes.toBytes(arr(0) + "-" + arr(1) + "-" + System.currentTimeMillis()))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(0)))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("job"), Bytes.toBytes(arr(1)))
        (new ImmutableBytesWritable, put)
        
      }
    }
    rdd.foreach { x => println(x) }
    try {
      rdd.saveAsHadoopDataset(jobConf)
    } catch {
      case ex: org.apache.hadoop.hbase.TableNotFoundException => {
        exit
      }
    }
    */

    val start_time = System.currentTimeMillis()
    println("start time is " + start_time)
    bankRDD.foreachPartition { x =>

      println("this is a new partition")
      var count = 0
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
      hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      val myTable = new HTable(hbaseconf, TableName.valueOf(tablename))
      myTable.setAutoFlush(false)
      myTable.setWriteBufferSize(3 * 1024 * 1024)
      x.foreach { y =>
        {
          println("this is new record")
          //println(y(0) + "..." + y(1))
          val p = new Put(Bytes.toBytes(y(0) + "-" + y(1) + "-" + System.currentTimeMillis()))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(y(0)))
          myTable.put(p)
          count = count + 1
          if (count > 2000) {
            myTable.flushCommits()
            count = 0
            println("start to flush cause count is more than 2000")
          }
        }
      }
      println("the last count is " + count)
      myTable.flushCommits()
      println("this is the last flush")
    }
    val end_time = System.currentTimeMillis()

    val total_time = (end_time - start_time) / 1000

    println("total spent time is " + total_time)

  }
}