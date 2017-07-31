package com.isesol.kafka
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.util._
object producer {
  def main(args:Array[String]){
    
    
    example("wang").test("wangjialong")
  }
}

case class example(args:String){
  def test(arg:String){
    println("this is " + arg + args)
  }
}