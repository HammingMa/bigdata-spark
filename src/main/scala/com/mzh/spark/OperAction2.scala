package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperAction2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("action 2").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[(Int, Int)] = sc.makeRDD(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val cntMap: collection.Map[Int, Long] = listRDD.countByKey()

    println(cntMap)


    listRDD.foreach(println)

    listRDD.foreachPartition(t => println(t.mkString(" ")))

  }
}
