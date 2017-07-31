package com.isesol.spark

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.sql.DriverManager
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.log4j.Logger

object jdbc {
  def main(args: Array[String]) {

    val driver = "org.apache.hive.jdbc.HiveDriver"
    val url_spark = "jdbc:hive2://10.215.4.165:10000/message"
    Class.forName(driver)
    val conn = DriverManager.getConnection(url_spark, "hadoop", "")
    val stmt = conn.createStatement()
    val sql = "select count(*) from cloudfactory.topic_5007"
    val starTime = System.currentTimeMillis()
    val rs = stmt.executeQuery(sql)

    while (rs.next()) {
      println(rs.getInt(1))
    }
    rs.close()
    stmt.close()
    conn.close()
  }
}