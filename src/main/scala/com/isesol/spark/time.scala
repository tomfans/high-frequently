package com.isesol.spark
import java.util.Date
import java.text.SimpleDateFormat  
object time {
  def main(args:Array[String]){
   
    val fm = new SimpleDateFormat("yyyy-mm-dd:HH:mm:ss")  
    println(fm.format(String.valueOf(new Date())))
  }
}