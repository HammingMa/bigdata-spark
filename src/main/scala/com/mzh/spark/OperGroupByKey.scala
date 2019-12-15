package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("one","two","three","one","two","one"))

    val mapRDD: RDD[(String, Int)] = listRDD.map((_,1))

    val groupByKeyRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    println(groupByKeyRDD.collect().mkString(" "))

    val result: RDD[(String, Int)] = groupByKeyRDD.map(t =>(t._1,t._2.sum))

    println(result.collect().mkString(" "))


    sc.stop()
  }
}
