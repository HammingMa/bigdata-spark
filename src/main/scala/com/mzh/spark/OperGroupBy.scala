package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperGroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_%2)

    println(groupByRDD.collect().mkString(" "))

    sc.stop()
  }
}
