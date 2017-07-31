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

object hbasescan {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("this is for spark SQL")
    conf.setSparkHome("d:\\spark_home")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    //hbaseconf.set("maxSessionTimeout", "6")
    val sc = new SparkContext(conf)
    try {

      val hbaseContext = new HBaseContext(sc, hbaseconf)
      val rowfilter = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("i517T510033")))
      val rowfilter1 = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("3C1610173")))
      val scan = new Scan()
      // scan.setRowPrefixFilter(Bytes.toBytes("i517T510033"))
      // scan.setRowPrefixFilter(Bytes.toBytes("3C1610173"))
      scan.setCaching(100)
      val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("total"), CompareOp.EQUAL, Bytes.toBytes("2"));
      val filterlist = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      val filterlist1 = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filterlist1.addFilter(rowfilter)
      filterlist1.addFilter(filter)

      filterlist.addFilter(rowfilter1)
      filterlist.addFilter(filterlist1)

      scan.setFilter(filterlist)
      val hbaserdd = hbaseContext.hbaseRDD(TableName.valueOf("t_device_fault_statistics"), scan)
      // hbaserdd.cache()
      println(hbaserdd.count())
      hbaserdd.foreach {
        case (_, result) =>
          val rowkey = Bytes.toString(result.getRow)
          val fault_level2_name = Bytes.toString(result.getValue("cf".getBytes, "fault_level2_name".getBytes))
          val total = Bytes.toString(result.getValue("cf".getBytes, "total".getBytes))
          println("the rowkey is:" + rowkey + "," + "the total is " + total)
      }

    } catch {
      case ex: Exception => println("can not connect hbase")
    }

    
    val htable = new HTable(hbaseconf, "t_device_fault_statistics")
    val scan1 = new Scan()
    //scan1.setCaching(3*1024*1024)
    val scaner = htable.getScanner(scan1)
    
    while(scaner.iterator().hasNext()){
       val result = scaner.next()
       if(result.eq(null)){

       } else {
         println(Bytes.toString(result.getRow) + "\t" + Bytes.toString(result.getValue("cf".getBytes, "fault_level2_name".getBytes)))
       }
    }

    scaner.close()
    htable.close()
    
  }
}