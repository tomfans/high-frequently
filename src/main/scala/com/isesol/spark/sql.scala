package com.isesol.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object sql {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("yarn-client").setAppName("this is for spark SQL")
    //conf.setSparkHome("d:\\spark_home")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val bankText = sc.textFile("hdfs://namenode01.isesol.com:8020/tmp/bank.csv").map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt)).toDF()
     
     bankText.registerTempTable("bank")
     val rs = sqlContext.sql("select * from bank where age > 80")
     rs.map(x => x.getAs[Int]("age") + "," + x.getAs[String]("job")).collect().foreach (println)
     
     val hctx = new org.apache.spark.sql.hive.HiveContext(sc)
     hctx.sql("select count(*) from datacenter.mysql_slow_query group by sql_text").collect().foreach(println)
  }
}

case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
