package com.iesol.high_frequency
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import scala.util.control._;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.isesol.mapreduce.binFileRead_forscala
import java.util.List;
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
import scala.util.Random

object parseFile {

  def main(args: Array[String]) {

    /*
     * fileName 文件名称
     * 
     * appId App请求发出者
     * 
     * bizId 业务唯一好吗
     * 
     * bizData 收集信息，包括字段,机床号 
     * 
     */
    val fileName = args(0)
    val appId = args(1)
    val machine_tool = args(2)
    val bizId = args(3)
    val bizData = args(4)

    //colId 表示目前只有一种高频采集，通过colID找到对应的表字段个数
    val colId = "1"

    println("fileName is " + fileName)
    println("bizId is " + bizId)
    println("machine_tool is " + machine_tool)
    
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("high frequency collection " + appId)
    val sc = new SparkContext(conf)
    val hbaseCols = binFileRead_forscala.getHaseCols(bizData)
    val total_colNums = hbaseCols.size()
    
    println("total cols number is " + total_colNums)
    val getFile = binFileRead_forscala.binFileOut(fileName, total_colNums)
    val getData = new Array[String](getFile.size())
    for (num <- 0 to getFile.size() - 1) {
      getData(num) = getFile.get(num)
    }

    val hbaseCols_scala = new Array[String](hbaseCols.size())

    for (num <- 0 to hbaseCols.size() - 1) {
      hbaseCols_scala(num) = hbaseCols.get(num)
      println("hbase cols is " + hbaseCols_scala(num))
    }

    val bankRDD = sc.parallelize(getData).map { x => x.split(",") }

    try {
      bankRDD.foreachPartition { x =>
        var count = 0
        val hbaseconf = HBaseConfiguration.create()
        hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
        hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseconf.set("maxSessionTimeout", "6")
        val myTable = new HTable(hbaseconf, TableName.valueOf("t_high_frequently"))
        // myTable.setAutoFlush(true)
        myTable.setWriteBufferSize(3 * 1024 * 1024)
        x.foreach { y =>
          {

            var rowkey = System.currentTimeMillis().toString()
            val p = new Put(Bytes.toBytes(machine_tool + "#" + bizId + "#" + rowkey))

            for (i <- 0 to hbaseCols_scala.size - 1) {
              p.add(Bytes.toBytes("cf"), Bytes.toBytes(hbaseCols_scala(i)), Bytes.toBytes(y(i)))
            }

            /* p.add(Bytes.toBytes("cf"), Bytes.toBytes("POSONSCREEN XC"), Bytes.toBytes(y(0)))
            p.add(Bytes.toBytes("cf"), Bytes.toBytes("AXFEEDBACKVEL X"), Bytes.toBytes(y(1)))
            p.add(Bytes.toBytes("cf"), Bytes.toBytes("AXFEEDBACKPOS X"), Bytes.toBytes(y(2))) */

            myTable.put(p)

          }

        }
        myTable.flushCommits()
        myTable.close()
      }
    } catch {
      case ex: Exception => println("can not connect hbase")
    }
  }
}
  

