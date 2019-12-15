package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperCogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val listRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6),(2,3)))

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = listRDD1.cogroup(listRDD2)



    println(cogroupRDD.collect().mkString(" "))

    sc.stop()
  }
}
