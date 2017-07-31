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
import org.apache.hadoop.hbase.client.ConnectionFactory
object faultCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    //conf.setMaster("yarn-client").setAppName("this is for spark SQL")
    conf.setSparkHome("/opt/cloudera/parcels/CDH/lib/spark")
    val sc = new SparkContext(conf)
    val hctx = new org.apache.spark.sql.hive.HiveContext(sc)
    import hctx.implicits._

    val res_device = hctx.sql("select mt_sn, case when fault_level2_name is NULL then '技术支持' else fault_level2_name end fault_level2_name , count(*) total from i5service.t_a_statistic where fault_report_time < now() and fault_report_time >= date_sub(now(),30) group by mt_sn, fault_level2_name")
    res_device.foreachPartition { x =>
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
      hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      val myTable = new HTable(hbaseconf, TableName.valueOf("t_device_fault_statistics"))
      myTable.setAutoFlush(false)
      x.foreach { y =>
        {
          val p = new Put(Bytes.toBytes(y(0).toString() + "#" + y(1)))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("fault_level2_name"), Bytes.toBytes(y(1).toString()))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("total"), Bytes.toBytes(y(2).toString()))
          myTable.put(p)
        }
      }
      myTable.flushCommits()
      myTable.close()
    }
    try {
      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
      config.set("hbase.zookeeper.property.clientPort", "2181")
      val connection = ConnectionFactory.createConnection(config)
      val admin = connection.getAdmin()
      admin.disableTable(TableName.valueOf("t_factory_fault_statistics"))
      admin.truncateTable(TableName.valueOf("t_factory_fault_statistics"), true)
      admin.disableTable(TableName.valueOf("t_device_fault_result_statistics"))
      admin.truncateTable(TableName.valueOf("t_device_fault_result_statistics"), true)
      //admin.enableTable(TableName.valueOf("t_factory_fault_statistics"))
      admin.close()
      connection.close()
    } catch {
      case ex: Exception => { println(ex) }
    }

    println("Start to deal with factory mode data")

    val res_factory = hctx.sql("select  a.use_right_owner_id,a.use_right_owner_name , case when b.fault_level2_name is NULL then '其他' else b.fault_level2_name end fault_level2_name ,count(*) from  (select a.use_right_owner_id,a.symgmachineid,b.symgm_name,a.use_right_owner_name from  i5service.t_symgmachines_attribution a join  i5service.t_symgmachines b on a.symgmachineid=b.symgmachineid and a.use_right_owner_id != '' and a.property_owner_name != ''  ) a  join i5service.t_a_statistic b on a.symgm_name=b.mt_sn and  fault_report_time < now() and fault_report_time >= date_sub(now(),30)  group by a.use_right_owner_id, a.use_right_owner_name, b.fault_level2_name")

    res_factory.foreachPartition { x =>
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
      hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      val myTable = new HTable(hbaseconf, TableName.valueOf("t_factory_fault_statistics"))
      myTable.setAutoFlush(false)
      x.foreach { y =>
        {
          val p = new Put(Bytes.toBytes(y(0).toString() + "#" + y(2)))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("fault_level2_name"), Bytes.toBytes(y(2).toString()))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("total"), Bytes.toBytes(y(3).toString()))
          myTable.put(p)
        }
      }
      myTable.flushCommits()
      myTable.close()
    }

    println("Start to deal with device fault mode data")

    val res_fault_result = hctx.sql("select mt_sn, service_result_name,service_result from i5service.t_a_statistic where fault_report_time < now() and fault_report_time >= date_sub(now(),30)")

    println(res_fault_result.count())
    
    res_fault_result.foreachPartition { x =>
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
      hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      val myTable = new HTable(hbaseconf, TableName.valueOf("t_device_fault_result_statistics"))
      myTable.setAutoFlush(false)
      x.foreach { y =>
        {
          val p = new Put(Bytes.toBytes(y(0).toString() + "#" + System.currentTimeMillis().toString()))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("service_result_name"), Bytes.toBytes(y(1).toString()))
          p.add(Bytes.toBytes("cf"), Bytes.toBytes("result_name_id"), Bytes.toBytes(y(2).toString()))
          myTable.put(p)
        }
      }
      myTable.flushCommits()
      myTable.close()

    }

    sc.stop()

  }
}

