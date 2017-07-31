package com.iesol.high_frequency

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
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.client.HTable

object parseHbaseCols {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("this is for spark SQL")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseconf.set("maxSessionTimeout", "6")
    val sc = new SparkContext(conf)
    try {
      val hbaseContext = new HBaseContext(sc, hbaseconf)
      val scan = new Scan()
      val get = new Get("1".getBytes())
      //scan.setRowPrefixFilter(Bytes.toBytes("i51530048-1007-9223370552914159518"))
      // scan.setCaching(100)
      // val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("age"), CompareOp.LESS, Bytes.toBytes("1"));
      // scan.setFilter(filter)
      val hbaserdd = hbaseContext.hbaseRDD(TableName.valueOf("tab_col_config"), scan)
      val table = new HTable(hbaseconf, "tab_col_config")
      val result = table.get(get)
      
      for(i <- 0 to result.listCells().size() - 1){
        println(new String(result.listCells().get(i).getQualifier))
      }
     
    /*  hbaserdd.foreach {
        case (_, result) =>
          val rowkey = Bytes.toString(result.getRow)
          val job = Bytes.toString(result.getValue("cf".getBytes, "param1".getBytes))
          val age = Bytes.toString(result.getValue("cf".getBytes, "param2".getBytes))
          println("the rowkey is:" + rowkey + "," + "the job is " + job + "," + "the age is " + age)
          
      } */
    
      //   println(hbaserdd.count()) 
      


    } catch {
      case ex: Exception => println("can not connect hbase")
    }
  }
}