package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object OperRepartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)

    val repartitionRDD: RDD[Int] = listRDD.repartition(2)

    repartitionRDD.glom().foreach{
        array => val s = array.mkString( " ")
      println(s)
    }
  }
}
