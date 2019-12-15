package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineageRelation {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("lineage")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[(Int, Int)] = sc.makeRDD(List((1,3),(2,4),(3,5)))

    val rRDD: RDD[(Int, Int)] = listRDD.reduceByKey(_+_)

    val mRDD: RDD[(Int, Int)] = rRDD.map(t =>(t._1,t._2*t._1))

    println(mRDD.toDebugString)
    println(mRDD.dependencies)
  }
}
