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

object hbase {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("yarn-client").setAppName("this is for spark SQL")
    //conf.setSparkHome("d:\\spark_home")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", "datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com")
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    val tablename = "test1"
    val jobConf = new JobConf(hbaseconf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // val hctx = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql
    val bankText = sc.textFile("hdfs://namenode01.isesol.com:8020/tmp/bank.csv").map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank1(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt)).toDF()

    val bankRDD = sc.textFile("hdfs://namenode01.isesol.com:8020/tmp/bank.csv")
    bankRDD.persist()

    bankText.registerTempTable("bank1")
    // sql("select * from test")
    //val rs = sqlContext.sql("select concat(age,'-',job,'-',unix_timestamp(current_timestamp())) as rowkey, age, job from bank1 where age > 80")
    //rs.map(x => x.getAs[Int]("age") + "," + x.getAs[String]("job")).collect().foreach (println)

    //val hctx = new org.apache.spark.sql.hive.HiveContext(sc)
    //val rs = hctx.sql("select  age, job from bank1 where age > 80")
    //hctx.sql("select count(*) from bank1 group by sql_text").collect().foreach(println)
    //rs.map(x => x.getAs[Int]("age") + "," + x.getAs[String]("job")).collect().foreach(println)

    val rdd = bankRDD.map(_.split(';')).map { arr =>
      {
        /*一个Put对象就是一行记录，在构造方法中指定主键  
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换  
       * Put.add方法接收三个参数：列族，列名，数据  
       */
        val put = new Put(Bytes.toBytes(arr(0) + "-" + arr(1) + "-" + System.currentTimeMillis()))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(0)))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("job"), Bytes.toBytes(arr(1)))
        (new ImmutableBytesWritable, put)
      }
    }

    rdd.foreach { x => println(x) }
    rdd.saveAsHadoopDataset(jobConf)

    val hbaseContext = new HBaseContext(sc, hbaseconf)
    val scan = new Scan()
    scan.setMaxVersions()
    //scan.setRowPrefixFilter(Bytes.toBytes("i51530048-1007-9223370552914159518"))
    scan.setCaching(100)

    val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("axabstpos_x"), CompareOp.EQUAL, Bytes.toBytes("7497.5830"));
    scan.setFilter(filter)
    val hbaserdd = hbaseContext.hbaseRDD(TableName.valueOf("t_principal_axes"), scan)
    
    hbaserdd.foreach {
      case (_, result) =>
        val rowkey = Bytes.toString(result.getRow)
        val job = Bytes.toString(result.getValue("cf".getBytes, "job".getBytes))
        val age = Bytes.toString(result.getValue("cf".getBytes, "age".getBytes))
       println("the rowkey is:" + rowkey + "," + "the job is " + job + "," + "the age is " + age)
    }  
    
    println(hbaserdd.count())

    sc.stop()
  }
}

case class Bank1(age: Integer, job: String, marital: String, education: String, balance: Integer)
