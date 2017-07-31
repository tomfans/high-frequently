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
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList

object filtertest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("this is for spark SQL")
    conf.setSparkHome("d:\\spark_home")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    val sc = new SparkContext(conf)
    try {

      val hbaseContext = new HBaseContext(sc, hbaseconf)
     // val rowfilter = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("A131420033-1007-9223370539574828268")))
    //  val rowfilter1 = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("3C1610173")))
      val scan = new Scan()

     // val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("total"), CompareOp.EQUAL, Bytes.toBytes("2"));

       //scan.setRowPrefixFilter(Bytes.toBytes("A131420033-1007-9223370539574828268"))
      // scan.setRowPrefixFilter(Bytes.toBytes("3C1610173"))
      //scan.setCaching(100)
      //val filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE)
     // filterlist.addFilter(filter)
     // filterlist.addFilter(rowfilter1)
      //scan.setFilter(rowfilter)
      val hbaserdd = hbaseContext.hbaseRDD(TableName.valueOf("t_axes_feeding_status"), scan)
      println(hbaserdd.count())

    } catch {
      case ex: Exception => println("can not connect hbase")
    }

  }
}