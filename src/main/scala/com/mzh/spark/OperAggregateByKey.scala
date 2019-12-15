package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    listRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val aggRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(math.max(_,_),_+_)

    aggRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val aggRDD1: RDD[(String, Int)] = listRDD.aggregateByKey(0)(_+_,_+_)

    aggRDD1.glom().collect().foreach(t => println(t.mkString(" ")))


    sc.stop()
  }
}
