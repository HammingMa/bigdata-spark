package com.mzh.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SeriFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("func")

    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("hadoop", "spark", "hive", "atguigu"))

    val search = new Search("h")

    val res3: RDD[String] = search.getMatch3(listRDD)

    println(res3.collect().mkString(" "))

    val res1: RDD[String] = search.getMatch1(listRDD)
    println(res1.collect().mkString(" "))

    val res2: RDD[String] = search.getMatch2(listRDD)

    println(res2.collect().mkString(" "))

    sc.stop()

  }
}

//class Search(q : String) extends java.io.Serializable{
class Search(q : String) {

  def isMatch(s :String): Boolean ={
    s.contains(q)
  }

  def getMatch1(rdd : RDD[String]): RDD[String] ={
    rdd.filter(isMatch)
  }

  def getMatch2(rdd : RDD[String]): RDD[String] ={
    rdd.filter(_.contains(q))
  }

  def getMatch3(rdd : RDD[String]): RDD[String] ={
    val m = this.q
    rdd.filter(_.contains(m))
  }
}
