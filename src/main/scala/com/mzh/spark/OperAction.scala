package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperAction {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

    val resReduce: Int = listRDD.reduce(_+_)
    println(resReduce)

    println(listRDD.collect().mkString(" "))

    println(listRDD.count())

    println(listRDD.first())

    val listRDD2: RDD[Int] = sc.makeRDD(List(3,5,1,2,4,7,0,9))

    println(listRDD2.take(3).mkString(" "))

    println(listRDD2.takeOrdered(3).mkString(" "))

    println(listRDD.aggregate(0)(_ + _, _ + _))
    println(listRDD.aggregate(10)(_ + _, _ + _))
    println(listRDD.fold(0)(_ + _))
  }
}
